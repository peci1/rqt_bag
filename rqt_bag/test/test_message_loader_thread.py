# Copyright 2026 rqt_bag maintainers
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#
#    * Neither the name of the copyright holder nor the names of its
#      contributors may be used to endorse or promote products derived from
#      this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.

import threading
import time
from types import SimpleNamespace

from rclpy.time import Time

from rqt_bag.message_loader_thread import MessageLoaderThread

TOPIC = '/chatter'
BASE_TIME_NS = 1_700_000_000_000_000_000
STEP_NS = 1_000_000_000
MESSAGES = {BASE_TIME_NS + i * STEP_NS: b'payload %d' % i for i in range(10)}
BAG = SimpleNamespace(bag_path='/fake/bag')


class FakeTimeline:
    """The subset of BagTimeline that MessageLoaderThread interacts with."""

    def __init__(self):
        self._playhead_generation = 0
        self._playhead_positions_cvs = {TOPIC: threading.Condition()}
        self._playhead_positions = {}
        self._messages_cvs = {TOPIC: threading.Condition()}
        self._messages = {}
        self._listeners = set()
        self._timeline_frame = SimpleNamespace(playhead=None)
        self.read_calls = []

    def get_entry(self, t, topic):
        assert topic == TOPIC
        position = None
        for timestamp in MESSAGES:
            if timestamp <= t.nanoseconds:
                position = timestamp
        if position is None:
            return None, None
        return BAG, SimpleNamespace(timestamp=position)

    def has_listeners(self, topic):
        return topic in self._listeners

    def read_message(self, bag, position, topic):
        self.read_calls.append(position)
        return MESSAGES[position]

    def set_playhead(self, timestamp_ns):
        self._timeline_frame.playhead = Time(nanoseconds=timestamp_ns)
        self._playhead_generation += 1
        for cv in self._playhead_positions_cvs.values():
            with cv:
                cv.notify_all()


def wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


class CountingDict(dict):
    """Counts assignments, e.g. to observe that a message is re-pushed."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_count = 0

    def __setitem__(self, key, value):
        self.set_count += 1
        super().__setitem__(key, value)


def message_loaded(timeline):
    bag, msg = timeline._messages.get(TOPIC, (None, None))
    return msg


def test_loader_loads_message_at_playhead():
    timeline = FakeTimeline()
    loader = MessageLoaderThread(timeline, TOPIC)
    try:
        timeline._listeners.add(TOPIC)
        timeline.set_playhead(BASE_TIME_NS + 3 * STEP_NS)
        assert wait_until(lambda: message_loaded(timeline) == b'payload 3')
        assert timeline._playhead_positions[TOPIC] == (BAG, BASE_TIME_NS + 3 * STEP_NS)
        assert timeline.read_calls == [BASE_TIME_NS + 3 * STEP_NS]
    finally:
        loader.stop()


def test_loader_skips_reloads_of_unchanged_position():
    timeline = FakeTimeline()
    loader = MessageLoaderThread(timeline, TOPIC)
    try:
        timeline._listeners.add(TOPIC)
        # Playhead moves within the interval of the same message
        timeline.set_playhead(BASE_TIME_NS + 3 * STEP_NS + 500_000_000)
        assert wait_until(lambda: message_loaded(timeline) == b'payload 3')
        timeline.set_playhead(BASE_TIME_NS + 3 * STEP_NS + 700_000_000)
        time.sleep(0.2)
        assert timeline.read_calls == [BASE_TIME_NS + 3 * STEP_NS]
    finally:
        loader.stop()


def test_loader_resolves_entry_before_first_message():
    timeline = FakeTimeline()
    loader = MessageLoaderThread(timeline, TOPIC)
    try:
        timeline._listeners.add(TOPIC)
        timeline.set_playhead(BASE_TIME_NS - STEP_NS)
        assert wait_until(lambda: TOPIC in timeline._playhead_positions)
        assert timeline._playhead_positions[TOPIC] == (None, None)
        assert timeline.read_calls == []
    finally:
        loader.stop()


def test_loader_without_listeners_records_position_only():
    timeline = FakeTimeline()
    loader = MessageLoaderThread(timeline, TOPIC)
    try:
        timeline.set_playhead(BASE_TIME_NS + 2 * STEP_NS)
        assert wait_until(lambda: TOPIC in timeline._playhead_positions)
        assert timeline.read_calls == []
        # Once a listener appears, the current message is loaded
        timeline._listeners.add(TOPIC)
        loader.reset()
        timeline.set_playhead(BASE_TIME_NS + 2 * STEP_NS)
        assert wait_until(lambda: message_loaded(timeline) == b'payload 2')
        assert timeline.read_calls == [BASE_TIME_NS + 2 * STEP_NS]
    finally:
        loader.stop()


def test_loader_reset_reloads_same_message():
    timeline = FakeTimeline()
    loader = MessageLoaderThread(timeline, TOPIC)
    try:
        timeline._listeners.add(TOPIC)
        timeline.set_playhead(BASE_TIME_NS + 5 * STEP_NS)
        assert wait_until(lambda: message_loaded(timeline) == b'payload 5')
        assert len(timeline.read_calls) == 1
        # A new view attaches: reset makes the loader push the same message
        # again (served from the message cache) to the new listener
        messages = CountingDict()
        messages.update(timeline._messages)
        timeline._messages = messages
        loader.reset()
        timeline.set_playhead(BASE_TIME_NS + 5 * STEP_NS)
        assert wait_until(lambda: messages.set_count >= 1)
        assert message_loaded(timeline) == b'payload 5'
        assert len(timeline.read_calls) == 1
    finally:
        loader.stop()
