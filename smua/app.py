import typing as t

from werkzeug import Request
from werkzeug.datastructures import MultiDict, FileStorage
from werkzeug.formparser import FormDataParser, MultiPartParser
from werkzeug.http import parse_options_header
from werkzeug.sansio.multipart import MultipartDecoder, Epilogue, NeedData, Data, File, Field
from werkzeug.wsgi import get_content_length, get_input_stream


def handle_file_upload(readable):
    while True:
        b = readable.read(100)
        if not b:
            break
        print(f"Got bytes: {b}")


class DecodedStream:
    READ_IN_CHUNKS_OF = 1024 * 1024 * 25  # 25 MB

    def __init__(self, stream: t.IO[bytes], boundary: bytes):
        self.stream = stream
        self.boundary = boundary
        self.decoder = MultipartDecoder(boundary)
        self.buffer = b""
        self.closed = False
        self._content_length = None

        # Read until we have the content length
        while self._content_length is None:
            self._read_into_stream(1024)

    def read(self, size: int) -> bytes:
        if self.closed or len(self.buffer) >= size:
            b = self.buffer[:size]
            self.buffer = self.buffer[len(b):]

            return b

        while not self.closed and len(self.buffer) <= size:
            self._read_into_stream(size)

        b = self.buffer[:size]
        self.buffer = self.buffer[len(b):]

        return b

    def seekable(self):
        return False

    def seek(self, *args, **kwargs):
        raise RuntimeError("Cannot use seek on this decoded stream.")

    @property
    def content_length(self):
        if self._content_length is None:
            raise ValueError("Content length must be provided")
        return self._content_length

    def _read_into_stream(self, size: int | None = None):
        if size is None:
            size = self.READ_IN_CHUNKS_OF
        data = self.stream.read(size)
        self.decoder.receive_data(data)
        event = self.decoder.next_event()

        while not isinstance(event, (Epilogue, NeedData)):
            if isinstance(event, File):
                content_length = event.headers.get("Content-Length")
                if content_length is None:
                    raise ValueError("Content-Length of file is required.")
                self._content_length = content_length
            elif isinstance(event, Data):
                self.buffer += event.data
                if not event.more_data:
                    self.closed = True

            event = self.decoder.next_event()

    def __sizeof__(self):
        return self.content_length


def convert_environ_into_stream(environ) -> t.IO[bytes]:
    """
    Parses an environ into a stream.

    The environ is expected to represent a multipart/form-data request. The
    first and only form element is expected to be 'file'.
    """

    # Ensure the request is a POST request
    if environ["REQUEST_METHOD"] != "POST":
        raise ValueError("Request method must be POST")

    # Ensure the content type is multipart/form-data
    mimetype, options = parse_options_header(environ.get("CONTENT_TYPE", ""))
    if mimetype != "multipart/form-data":
        raise ValueError("Content type must be multipart/form-data")

    boundary = options.get("boundary", "").encode("ascii")
    if not boundary:
        raise ValueError("Missing boundary")

    # Get the content length
    content_length = get_content_length(environ)
    if content_length is None:
        raise ValueError("Content length must be provided")

    stream = get_input_stream(environ)
    decoded_stream = t.cast(t.IO[bytes], DecodedStream(stream, boundary))

    return decoded_stream


def application(environ, start_response):
    decoded_stream = convert_environ_into_stream(environ)
    handle_file_upload(decoded_stream)
    start_response("200 OK", [("Content-Type", "text/plain")])
    return ["Hello World!".encode("utf-8")]

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    run_simple("127.0.0.1", 5000, application, use_debugger=True)
