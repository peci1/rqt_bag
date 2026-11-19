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
#    * Neither the name of the rqt_bag maintainers nor the names of its
#      contributors may be used to endorse or promote products derived from
#      this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import gc
import os
import threading
import time
from types import SimpleNamespace

import pytest
from rclpy.serialization import serialize_message
from rclpy.time import Time

import rosbag2_py

from rqt_bag.bag_timeline import BagTimeline
from rqt_bag.bag_widget import BagWidget

from std_msgs.msg import String

TOPIC = '/chatter'
DATATYPE = 'std_msgs/msg/String'
BASE_TIME_NS = 1_700_000_000_000_000_000  # 2023-11-14T22:13:20Z


class FakeBag:
    """A minimal bag for driving BagTimeline without a bag on disk."""

    def __init__(self, topics_by_type, bag_path):
        self._topics_by_type = topics_by_type
        self.bag_path = bag_path

    def get_topics(self):
        return [topic for topics in self._topics_by_type.values() for topic in topics]

    def size(self):
        return 16

    def get_topic_type(self, topic):
        for datatype, topics in self._topics_by_type.items():
            if topic in topics:
                return datatype
        return None

    def get_topics_by_type(self):
        return {datatype: list(topics) for datatype, topics in self._topics_by_type.items()}

    def get_earliest_timestamp(self):
        return Time(nanoseconds=BASE_TIME_NS)

    def get_latest_timestamp(self):
        return Time(nanoseconds=BASE_TIME_NS + 1)

    def entries_in_range_generator(self, start_stamp, end_stamp, topics):
        # The index cache thread indexes this bag right after add_bag.
        return iter([])


def make_context():
    return SimpleNamespace(
        node=None, serial_number=lambda: 1, remove_widget=lambda widget: None)


@pytest.fixture(scope='module')
def qapp():
    os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')
    from python_qt_binding.QtWidgets import QApplication
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.fixture
def fake_bag(tmp_path):
    bag_path = tmp_path / 'fake.bag'
    bag_path.write_bytes(b'x' * 16)
    return FakeBag({DATATYPE: [TOPIC]}, str(bag_path))


@pytest.fixture
def timeline(qapp):
    timeline = BagTimeline(make_context(), False)
    yield timeline
    timeline.handle_close()


@pytest.fixture
def bag_widget(qapp):
    widget = BagWidget(make_context(), False)
    yield widget
    widget.shutdown_all()


def wait_until(condition, timeout=10.0, message='condition not met in time'):
    deadline = time.perf_counter() + timeout
    while not condition():
        assert time.perf_counter() < deadline, message
        time.sleep(0.01)


def test_get_datatype_does_not_block_while_indexing(timeline, fake_bag):
    """Painting the timeline must not wait for the indexing to release the bag lock."""
    timeline.add_bag(fake_bag)
    assert timeline.get_datatype(TOPIC) == DATATYPE

    # The index cache thread holds _bag_lock for the whole indexing of a bag;
    # simulate that with a thread that keeps the lock until released.
    lock_held = threading.Event()
    release_lock = threading.Event()

    def hold_lock():
        with timeline._bag_lock:
            lock_held.set()
            release_lock.wait(10.0)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    try:
        assert lock_held.wait(5.0)
        start = time.perf_counter()
        assert timeline.get_datatype(TOPIC) == DATATYPE
        assert time.perf_counter() - start < 0.5
    finally:
        release_lock.set()
        holder.join()


def test_get_datatype_of_unknown_topic_is_none(timeline, fake_bag):
    timeline.add_bag(fake_bag)
    assert timeline.get_datatype('/does_not_exist') is None


def test_adding_bag_with_conflicting_datatype_raises(timeline, fake_bag, tmp_path):
    timeline.add_bag(fake_bag)

    other_path = tmp_path / 'other.bag'
    other_path.write_bytes(b'x' * 16)
    other_bag = FakeBag({'builtin_interfaces/msg/Int32': [TOPIC]}, str(other_path))
    with pytest.raises(Exception, match='multiple datatypes'):
        timeline.add_bag(other_bag)


def test_status_bar_shows_indexing_progress_before_playhead_is_set(bag_widget):
    # No bag is loaded, so there is no playhead yet.
    assert bag_widget._timeline._timeline_frame.playhead is None

    bag_widget._timeline.background_progress = 42
    bag_widget._update_status_bar()
    assert bag_widget.progress_bar.value() == 42
    assert bag_widget.progress_bar.isTextVisible()
    assert bag_widget.progress_bar.format() == 'Indexing %p%'

    # ... and the progress is hidden again once the indexing is done.
    bag_widget._timeline.background_progress = 0
    bag_widget._update_status_bar()
    assert not bag_widget.progress_bar.isTextVisible()


def test_status_text_overrides_indexing_progress(bag_widget):
    bag_widget._set_status_text("Loading '/some/bag' ...")
    bag_widget._timeline.background_progress = 50
    bag_widget._update_status_bar()
    assert bag_widget.progress_bar.format() == "Loading '/some/bag' ..."
    assert bag_widget.progress_bar.value() == 50
    bag_widget._set_status_text('')


@pytest.fixture
def tiny_bag(tmp_path):
    bag_dir = str(tmp_path / 'tiny_bag')
    writer = rosbag2_py.SequentialWriter()
    writer.open(rosbag2_py.StorageOptions(uri=bag_dir, storage_id='mcap'),
                rosbag2_py.ConverterOptions())
    writer.create_topic(rosbag2_py.TopicMetadata(
        id=0, name=TOPIC, type=DATATYPE, serialization_format='cdr'))
    msg = String()
    msg.data = 'hello'
    writer.write(TOPIC, serialize_message(msg), BASE_TIME_NS)
    del writer
    gc.collect()
    return bag_dir


def test_load_bag_opens_the_bag_in_background(bag_widget, tiny_bag, qapp):
    # load_bag only enqueues the filename, so the GUI thread stays responsive
    # even while large bags are being opened.
    start = time.perf_counter()
    bag_widget.load_bag(tiny_bag)
    assert time.perf_counter() - start < 0.5

    def bag_loaded():
        qapp.processEvents()  # deliver the queued bag_opened signal
        return bool(bag_widget._timeline._bags)

    wait_until(bag_loaded, message='the bag was not loaded in the background')

    assert len(bag_widget._timeline._bags) == 1
    assert bag_widget._timeline.get_datatype(TOPIC) == DATATYPE
    assert bag_widget._timeline._timeline_frame.playhead is not None
    assert bag_widget._status_text == ''


def test_load_bag_failure_keeps_the_ui_usable(bag_widget, qapp):
    bag_widget.load_bag('/does/not/exist')

    def failed():
        qapp.processEvents()
        return 'failed' in bag_widget._status_text

    wait_until(failed, message='the load failure was not reported')

    assert bag_widget._timeline._bags == []
    assert 'failed' in bag_widget._status_text
