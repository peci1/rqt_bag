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

import pytest
from rclpy.serialization import serialize_message
from rclpy.time import Time

import rosbag2_py

from rqt_bag.mcap_index import McapIndex
from rqt_bag.rosbag2 import Rosbag2
from std_msgs.msg import String

TOPIC_A = '/alpha'
TOPIC_B = '/beta'
BASE_TIME_NS = 1_700_000_000_000_000_000  # 2023-11-14T22:13:20Z
STEP_NS = 1_000_000_000


def write_bag(path, messages, compression_options=None, storage_config=None):
    """Write a bag of (topic, String data, timestamp) tuples, return its path."""
    writer = (rosbag2_py.SequentialCompressionWriter(compression_options)
              if compression_options else rosbag2_py.SequentialWriter())
    storage_options = rosbag2_py.StorageOptions(uri=path, storage_id='mcap')
    if storage_config is not None:
        config_path = f'{path}_storage_config.yaml'
        with open(config_path, 'w') as config_file:
            for key, value in storage_config.items():
                config_file.write(f'{key}: {value}\n')
        storage_options.storage_config_uri = config_path
    writer.open(storage_options, rosbag2_py.ConverterOptions())
    topics = {topic for topic, _, _ in messages}
    for topic in topics:
        writer.create_topic(rosbag2_py.TopicMetadata(
            id=0, name=topic, type='std_msgs/msg/String', serialization_format='cdr'))
    for topic, data, timestamp in messages:
        msg = String()
        msg.data = data
        writer.write(topic, serialize_message(msg), timestamp)
    del writer
    gc.collect()
    return path


def make_messages():
    """Interleave two topics; include out-of-order timestamps within a topic."""
    messages = []
    for i in range(30):
        messages.append((TOPIC_A, f'alpha {i}', BASE_TIME_NS + i * STEP_NS))
        messages.append((TOPIC_B, f'beta {i}',
                         BASE_TIME_NS + (2 * i + 1) * STEP_NS + STEP_NS // 2))
    # Written last, but sorts into the middle of topic B: message index
    # entries are not guaranteed to arrive in timestamp order.
    messages.append((TOPIC_B, 'beta early', BASE_TIME_NS + 20 * STEP_NS + STEP_NS // 4))
    return messages


MESSAGES = make_messages()


@pytest.fixture
def plain_bag(tmp_path):
    return write_bag(str(tmp_path / 'plain'), MESSAGES)


@pytest.fixture
def chunk_compressed_bag(tmp_path):
    # Chunk compression is configured through the storage config of the mcap plugin
    return write_bag(str(tmp_path / 'chunk_compressed'), MESSAGES,
                     storage_config={'compression': 'zstd'})


@pytest.fixture
def message_compressed_bag(tmp_path):
    return write_bag(
        str(tmp_path / 'message_compressed'), MESSAGES,
        rosbag2_py.CompressionOptions(
            compression_format='zstd', compression_mode=rosbag2_py.CompressionMode.MESSAGE))


def expected_messages():
    """Return the expected sorted (timestamp, serialized data) per topic."""
    expected = {TOPIC_A: [], TOPIC_B: []}
    for topic, data, timestamp in MESSAGES:
        msg = String()
        msg.data = data
        expected[topic].append((timestamp, serialize_message(msg)))
    for topic in expected:
        expected[topic].sort()
    return expected


def mcap_files(bag_dir):
    return sorted(
        os.path.join(bag_dir, name) for name in os.listdir(bag_dir) if name.endswith('.mcap'))


@pytest.mark.parametrize('bag_fixture', ['plain_bag', 'chunk_compressed_bag',
                                         'message_compressed_bag'])
def test_index_timestamps_and_payloads(bag_fixture, request):
    bag_dir = request.getfixturevalue(bag_fixture)
    index = McapIndex(mcap_files(bag_dir))
    expected = expected_messages()
    for topic, expected_entries in expected.items():
        stamps = index.timestamps(topic)
        assert list(stamps) == [ts for ts, _ in expected_entries]
        for (expected_ts, expected_data) in expected_entries:
            payload, ts = index.entry_at_or_before(topic, expected_ts)
            assert ts == expected_ts
            assert payload == expected_data
        # Reading through entry_after must produce the same payloads
        last_ts = None
        for (expected_ts, expected_data) in expected_entries:
            if last_ts is None:
                hit = index.entry_after(topic, expected_ts - 1)
            else:
                hit = index.entry_after(topic, last_ts)
            assert hit is not None
            payload, ts = hit
            assert ts == expected_ts
            assert payload == expected_data
            last_ts = ts
        assert index.entry_after(topic, last_ts) is None


def test_index_boundaries(plain_bag):
    index = McapIndex(mcap_files(plain_bag))
    first_ts = index.timestamps(TOPIC_A)[0]
    last_ts = index.timestamps(TOPIC_A)[-1]
    assert index.entry_at_or_before(TOPIC_A, first_ts - 1) is None
    assert index.entry_at_or_before(TOPIC_A, first_ts)[1] == first_ts
    # A stamp between two messages resolves to the earlier one
    assert index.entry_at_or_before(TOPIC_A, first_ts + STEP_NS // 2)[1] == first_ts
    assert index.entry_at_or_before(TOPIC_A, last_ts + 10)[1] == last_ts
    assert index.entry_after(TOPIC_A, last_ts) is None
    assert 'not_a_topic' not in index
    assert list(index.timestamps_in_range(TOPIC_A, first_ts, first_ts + STEP_NS)) == \
        [first_ts, first_ts + STEP_NS]


def test_index_multiple_files(plain_bag, tmp_path):
    """The index merges the topics of several files sorted by timestamp."""
    files = mcap_files(plain_bag)
    second_dir = str(tmp_path / 'second')
    write_bag(second_dir, [(TOPIC_A, 'later', BASE_TIME_NS + 100 * STEP_NS)])
    index = McapIndex(files + mcap_files(second_dir))
    stamps = list(index.timestamps(TOPIC_A))
    assert stamps == sorted(stamps)
    assert stamps[-1] == BASE_TIME_NS + 100 * STEP_NS
    payload, ts = index.entry_at_or_before(TOPIC_A, BASE_TIME_NS + 100 * STEP_NS)
    assert ts == BASE_TIME_NS + 100 * STEP_NS


@pytest.mark.parametrize('bag_fixture', ['plain_bag', 'chunk_compressed_bag',
                                         'message_compressed_bag'])
def test_rosbag2_get_entry_matches_plugin(bag_fixture, request):
    bag_dir = request.getfixturevalue(bag_fixture)
    bag = Rosbag2(bag_dir)
    expected = expected_messages()
    expected_data = {}
    for topic, data, timestamp in MESSAGES:
        expected_data[timestamp] = data
    for topic, expected_entries in expected.items():
        for probe in [0, len(expected_entries) // 2, len(expected_entries) - 1]:
            probe_ts = expected_entries[probe][0]
            entry = bag.get_entry(Time(nanoseconds=probe_ts), topic)
            assert entry is not None
            assert entry.timestamp == probe_ts
            assert entry.data == expected_entries[probe][1]
            msg, _ = bag.deserialize_entry(entry)
            assert msg.data == expected_data[probe_ts]


@pytest.mark.parametrize('bag_fixture', ['plain_bag', 'chunk_compressed_bag',
                                         'message_compressed_bag'])
def test_entries_timestamps_in_range_matches_generator(bag_fixture, request):
    bag_dir = request.getfixturevalue(bag_fixture)
    bag = Rosbag2(bag_dir)
    start = Time(nanoseconds=BASE_TIME_NS)
    end = Time(nanoseconds=BASE_TIME_NS + 10 * STEP_NS)
    fast = bag.entries_timestamps_in_range(start, end, [TOPIC_A, TOPIC_B])
    assert fast is not None
    slow = {}
    for entry in bag.entries_in_range_generator(start, end, [TOPIC_A, TOPIC_B]):
        slow.setdefault(entry.topic, []).append(entry.timestamp)
    assert {t: list(s) for t, s in fast.items()} == slow


def test_non_mcap_bag_has_no_fast_path(tmp_path):
    bag_dir = str(tmp_path / 'sqlite')
    messages = [message for message in MESSAGES if message[0] == TOPIC_A][:2]
    writer = rosbag2_py.SequentialWriter()
    writer.open(rosbag2_py.StorageOptions(uri=bag_dir, storage_id='sqlite3'),
                rosbag2_py.ConverterOptions())
    writer.create_topic(rosbag2_py.TopicMetadata(
        id=0, name=TOPIC_A, type='std_msgs/msg/String', serialization_format='cdr'))
    for topic, data, timestamp in messages:
        msg = String()
        msg.data = data
        writer.write(topic, serialize_message(msg), timestamp)
    del writer
    gc.collect()
    bag = Rosbag2(bag_dir)
    assert bag.metadata.storage_identifier == 'sqlite3'
    assert bag.entries_timestamps_in_range(
        Time(nanoseconds=BASE_TIME_NS), Time(nanoseconds=BASE_TIME_NS + STEP_NS),
        [TOPIC_A]) is None
    # The plugin fallback still works
    entry = bag.get_entry(Time(nanoseconds=BASE_TIME_NS), TOPIC_A)
    assert entry is not None
    assert entry.topic == TOPIC_A
