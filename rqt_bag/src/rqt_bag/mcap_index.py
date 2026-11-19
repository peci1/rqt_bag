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


"""
Minimal read-only MCAP index used to speed up rqt_bag for MCAP bags.

Reading the message timestamps of an MCAP bag through the rosbag2 storage
plugin decompresses and copies the full message payloads of the bag, which
can take minutes for large bags. The MCAP format already stores a compact
message index (a small fraction of the file size) with the timestamp and
location of every message, and this module parses that index directly:

* the footer and summary section are parsed for the chunk table and the
  topic (channel) list,
* the message index records attached to every chunk are parsed into
  per-topic timestamp lists,
* individual messages are read on demand by seeking into the chunk that
  contains them and decompressing only that chunk.

This module deliberately implements only the small subset of MCAP that
rosbag2 writes; anything unexpected raises, and the caller is expected to
fall back to the regular storage plugin reader.
"""

from array import array
import bisect
import ctypes
import ctypes.util
import os
import struct
import threading

MCAP_MAGIC = b'\x89MCAP0\r\n'

OP_CHANNEL = 0x04
OP_MESSAGE = 0x05
OP_CHUNK = 0x06
OP_MESSAGE_INDEX = 0x07
OP_CHUNK_INDEX = 0x08

ZSTD_MAGIC = b'\x28\xb5\x2f\xfd'
ZSTD_CONTENTSIZE_ERROR = 0xFFFFFFFFFFFFFFFE
ZSTD_CONTENTSIZE_UNKNOWN = 0xFFFFFFFFFFFFFFFF

# Number of decompressed chunks kept in memory. Playback reads messages
# sequentially from one chunk at a time, so a small cache is enough.
CHUNK_CACHE_SIZE = 4

_UNPACK_U16 = struct.Struct('<H').unpack_from
_UNPACK_U32 = struct.Struct('<I').unpack_from
_UNPACK_U64 = struct.Struct('<Q').unpack_from


class McapIndexError(Exception):
    """Raised when a file does not contain the MCAP structures this module expects."""


def _load_libzstd():
    for name in ('libzstd.so.1', 'libzstd.so', 'zstd.dll', 'libzstd.dylib'):
        try:
            return ctypes.CDLL(name)
        except OSError:
            continue
    path = ctypes.util.find_library('zstd')
    return ctypes.CDLL(path) if path else None


_LIBZSTD = None
_LIBZSTD_ERROR = None


def _zstd_decompress(data):
    """Decompress a zstd frame that declares its content size."""
    global _LIBZSTD, _LIBZSTD_ERROR
    if _LIBZSTD is None:
        if _LIBZSTD_ERROR is not None:
            raise McapIndexError(f'libzstd unavailable: {_LIBZSTD_ERROR}')
        try:
            _LIBZSTD = _load_libzstd()
        except OSError as e:
            _LIBZSTD_ERROR = str(e)
            raise McapIndexError(f'libzstd unavailable: {e}') from e
        if _LIBZSTD is None:
            _LIBZSTD_ERROR = 'not found'
            raise McapIndexError('libzstd unavailable: not found')
        _LIBZSTD.ZSTD_getFrameContentSize.restype = ctypes.c_ulonglong
        _LIBZSTD.ZSTD_getFrameContentSize.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
        _LIBZSTD.ZSTD_decompress.restype = ctypes.c_size_t
        _LIBZSTD.ZSTD_decompress.argtypes = \
            [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.c_size_t]
        _LIBZSTD.ZSTD_isError.restype = ctypes.c_uint
        _LIBZSTD.ZSTD_isError.argtypes = [ctypes.c_size_t]

    lib = _LIBZSTD
    content_size = lib.ZSTD_getFrameContentSize(data, len(data))
    if content_size in (ZSTD_CONTENTSIZE_ERROR, ZSTD_CONTENTSIZE_UNKNOWN):
        raise McapIndexError('zstd frame without content size')
    if content_size == 0:
        return b''
    dst = ctypes.create_string_buffer(int(content_size))
    written = lib.ZSTD_decompress(dst, len(dst), data, len(data))
    if lib.ZSTD_isError(written):
        raise McapIndexError('zstd decompression failed')
    return dst.raw[:written]


class _Chunk:
    """A chunk record from the summary section plus its message index location."""

    __slots__ = ('start', 'length', 'message_index_start', 'message_index_length')

    def __init__(self, start, length, message_index_start, message_index_length):
        self.start = start
        self.length = length
        self.message_index_start = message_index_start
        self.message_index_length = message_index_length


class _McapFile:
    """Parses the index structures of a single MCAP file and reads messages on demand."""

    def __init__(self, path):
        self.path = path
        self.fd = os.open(path, os.O_RDONLY)
        self.file_size = os.fstat(self.fd).st_size
        self._chunk_cache = {}
        self._chunk_cache_order = []
        self._lock = threading.Lock()

        chunk_indexes, channels = self._parse_summary()
        self.chunks = [
            _Chunk(start, length, min(message_index_offsets.values())
                   if message_index_offsets else None, message_index_length)
            for (start, length, message_index_offsets, message_index_length) in chunk_indexes]
        # channel id -> topic name
        self.channels = channels
        # topic name -> list of (timestamp, chunk_index, offset in chunk records)
        self.topic_entries = {}

    def close(self):
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def _read_at(self, offset, size):
        data = os.pread(self.fd, size, offset)
        if len(data) != size:
            raise McapIndexError(
                f'{self.path}: unexpected end of file at offset {offset}')
        return data

    def _parse_summary(self):
        if self.file_size < len(MCAP_MAGIC) + 29 + len(MCAP_MAGIC):
            raise McapIndexError(f'{self.path}: file too small')
        if os.pread(self.fd, len(MCAP_MAGIC), 0) != MCAP_MAGIC or \
                os.pread(self.fd, len(MCAP_MAGIC), self.file_size - len(MCAP_MAGIC)) != MCAP_MAGIC:
            raise McapIndexError(f'{self.path}: not an MCAP file')

        footer = self._read_at(self.file_size - len(MCAP_MAGIC) - 29, 29)
        opcode = footer[0]
        length = _UNPACK_U64(footer, 1)[0]
        if opcode != 0x02 or length != 20:
            raise McapIndexError(f'{self.path}: invalid footer')
        summary_start, summary_offset_start = struct.unpack_from('<QQ', footer, 9)
        if summary_start == 0:
            raise McapIndexError(f'{self.path}: file has no summary section')

        summary_end = summary_offset_start if summary_offset_start != 0 \
            else self.file_size - len(MCAP_MAGIC) - 29
        chunk_indexes = []
        channels = {}
        pos = summary_start
        while pos < summary_end:
            header = self._read_at(pos, 9)
            opcode = header[0]
            length = _UNPACK_U64(header, 1)[0]
            data = self._read_at(pos + 9, length)
            if opcode == OP_CHUNK_INDEX:
                message_start, message_end, chunk_start, chunk_length = \
                    struct.unpack_from('<QQQQ', data, 0)
                offset = 32
                map_length = _UNPACK_U32(data, offset)[0]
                offset += 4
                message_index_offsets = dict(
                    struct.iter_unpack('<HQ', data[offset:offset + map_length]))
                offset += map_length
                message_index_length = _UNPACK_U64(data, offset)[0]
                if message_end < message_start or chunk_length == 0:
                    raise McapIndexError(f'{self.path}: invalid chunk index')
                chunk_indexes.append(
                    (chunk_start, chunk_length, message_index_offsets, message_index_length))
            elif opcode == OP_CHANNEL:
                channel_id = _UNPACK_U16(data, 0)[0]
                topic_length = _UNPACK_U32(data, 4)[0]
                channels[channel_id] = data[8:8 + topic_length].decode()
            pos += 9 + length
        return chunk_indexes, channels

    def parse_message_indexes(self):
        """Fill topic_entries with (timestamp, chunk_index, offset) per topic."""
        topic_entries = {}
        for chunk_index, chunk in enumerate(self.chunks):
            if chunk.message_index_start is None or chunk.message_index_length == 0:
                continue
            block = self._read_at(chunk.message_index_start, chunk.message_index_length)
            pos = 0
            while pos < len(block):
                if block[pos] != OP_MESSAGE_INDEX:
                    raise McapIndexError(f'{self.path}: invalid message index record')
                length = _UNPACK_U64(block, pos + 1)[0]
                data_start = pos + 9
                channel_id = _UNPACK_U16(block, data_start)[0]
                map_length = _UNPACK_U32(block, data_start + 2)[0]
                pairs_start = data_start + 6
                pairs_end = pairs_start + map_length
                if pairs_end > data_start + length or map_length % 16 != 0:
                    raise McapIndexError(f'{self.path}: invalid message index record')
                topic = self.channels.get(channel_id)
                if topic is None:
                    raise McapIndexError(f'{self.path}: unknown channel {channel_id}')
                entries = topic_entries.setdefault(topic, [])
                for timestamp, offset in struct.iter_unpack(
                        '<QQ', block[pairs_start:pairs_end]):
                    entries.append((timestamp, chunk_index, offset))
                pos = data_start + length
        self.topic_entries = topic_entries

    def _read_chunk_records(self, chunk_index):
        """Return the (decompressed) records region of a chunk, caching recent chunks."""
        with self._lock:
            records = self._chunk_cache.get(chunk_index)
        if records is not None:
            return records

        chunk = self.chunks[chunk_index]
        # Chunk record: opcode, length, message start/end times, uncompressed
        # size, CRC of the uncompressed records, compression string
        # (length-prefixed), records length, then the records themselves.
        header = self._read_at(chunk.start, 9 + 8 + 8 + 8 + 4 + 4 + 8 + 8)
        if header[0] != OP_CHUNK:
            raise McapIndexError(f'{self.path}: invalid chunk record')
        offset = 9 + 8 + 8
        offset += 8  # uncompressed size, validated below against the records
        offset += 4  # CRC of the uncompressed records
        compression_length = _UNPACK_U32(header, offset)[0]
        if compression_length > 8:
            raise McapIndexError(f'{self.path}: invalid chunk compression field')
        offset += 4
        compression = header[offset:offset + compression_length].decode()
        offset += compression_length
        records_length = _UNPACK_U64(header, offset)[0]
        offset += 8
        records_start = chunk.start + offset
        if records_start + records_length > chunk.start + chunk.length:
            raise McapIndexError(f'{self.path}: chunk records exceed the chunk record')

        records = self._read_at(records_start, records_length)
        if compression == 'zstd':
            records = _zstd_decompress(records)
        elif compression not in ('', 'none'):
            raise McapIndexError(f'{self.path}: unsupported chunk compression {compression!r}')

        with self._lock:
            self._chunk_cache[chunk_index] = records
            self._chunk_cache_order.append(chunk_index)
            while len(self._chunk_cache_order) > CHUNK_CACHE_SIZE:
                self._chunk_cache.pop(self._chunk_cache_order.pop(0), None)
        return records

    def read_message(self, chunk_index, offset):
        """Return the payload of the message at the given chunk-relative offset."""
        records = self._read_chunk_records(chunk_index)
        if offset + 9 + 22 > len(records):
            raise McapIndexError(f'{self.path}: message offset out of chunk bounds')
        if records[offset] != OP_MESSAGE:
            raise McapIndexError(f'{self.path}: message offset does not point at a message')
        length = _UNPACK_U64(records, offset + 1)[0]
        payload_start = offset + 9 + 22
        payload_end = payload_start + length - 22
        if payload_end > len(records) or payload_end < payload_start:
            raise McapIndexError(f'{self.path}: message length out of chunk bounds')
        payload = records[payload_start:payload_end]
        if payload[:4] == ZSTD_MAGIC:
            # Bags recorded with per-message compression store the compressed
            # payload directly in the chunk records.
            payload = _zstd_decompress(payload)
        return payload


class McapIndex:
    """
    Combined message index of all MCAP files of one bag, keyed by topic.

    For every topic it keeps three parallel arrays: message timestamps,
    per-message file/chunk references, and chunk-relative message offsets.
    """

    def __init__(self, paths):
        files = [_McapFile(path) for path in paths]
        try:
            for f in files:
                f.parse_message_indexes()
        except Exception:
            for f in files:
                f.close()
            raise

        self.files = files
        self._stamps = {}
        self._where = {}
        self._offsets = {}
        topics = set()
        for f in files:
            topics.update(f.topic_entries)
        for topic in topics:
            entries = []
            for file_index, f in enumerate(files):
                for timestamp, chunk_index, offset in f.topic_entries.get(topic, ()):
                    entries.append(
                        (timestamp, (file_index << 32) | chunk_index, offset))
            entries.sort(key=lambda entry: (entry[0], entry[1], entry[2]))
            stamps = array('q', [e[0] for e in entries])
            where = array('q', [e[1] for e in entries])
            offsets = array('q', [e[2] for e in entries])
            self._stamps[topic] = stamps
            self._where[topic] = where
            self._offsets[topic] = offsets

    def close(self):
        for f in self.files:
            f.close()

    def __contains__(self, topic):
        return topic in self._stamps

    def __len__(self):
        return len(self._stamps)

    def topics(self):
        return list(self._stamps)

    def timestamps(self, topic):
        """Return the sorted message timestamps of a topic, in nanoseconds."""
        return self._stamps.get(topic, ())

    def timestamps_in_range(self, topic, start_ns, end_ns):
        """Return the sorted timestamps of a topic in [start_ns, end_ns]."""
        stamps = self._stamps.get(topic)
        if stamps is None:
            return ()
        lo = bisect.bisect_left(stamps, start_ns)
        hi = bisect.bisect_right(stamps, end_ns, lo=lo)
        return stamps[lo:hi]

    def _read_message(self, topic, position):
        where = self._where[topic][position]
        offset = self._offsets[topic][position]
        file_index = where >> 32
        chunk_index = where & 0xFFFFFFFF
        payload = self.files[file_index].read_message(chunk_index, offset)
        return payload

    def entry_at_or_before(self, topic, timestamp):
        """Return (payload, timestamp) of the last message at or before timestamp."""
        stamps = self._stamps.get(topic)
        if stamps is None:
            return None
        position = bisect.bisect_right(stamps, timestamp) - 1
        if position < 0:
            return None
        return self._read_message(topic, position), stamps[position]

    def entry_after(self, topic, timestamp):
        """Return (payload, timestamp) of the first message after timestamp."""
        stamps = self._stamps.get(topic)
        if stamps is None:
            return None
        position = bisect.bisect_right(stamps, timestamp)
        if position >= len(stamps):
            return None
        return self._read_message(topic, position), stamps[position]
