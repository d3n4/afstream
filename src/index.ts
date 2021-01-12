import { PathLike } from 'fs';
import fs, { FileHandle } from 'fs/promises';

type FileStreamOptions = {
  bufferSize?: number;
  bufferLimit?: number;
  flags?: string;
  terminator?: string | Buffer;
  includeTerminator?: boolean;
  transform?: (e: Buffer) => Promise<any>;
};

export async function* afstream(path: FileHandle | PathLike, options?: FileStreamOptions): AsyncGenerator<Buffer> {
  options = options || {};

  const bufferLimit = options.bufferLimit || 0;
  const terminator = options.terminator || '\n';
  const includeTerminator = options.includeTerminator || false;
  const transform = options.transform || (async (e) => e);
  const flags = options.flags || 'r';
  let bufferSize = options.bufferSize || 0;

  let handle: FileHandle;

  if (Buffer.isBuffer(path)) {
    handle = await fs.open(path.toString(), flags);
  } else if (typeof path === 'string') {
    handle = await fs.open(path, flags);
  } else if (typeof (path as any).fd !== 'undefined') {
    handle = path as FileHandle;
  } else {
    throw new Error('Invalid file handle');
  }

  const stat = await handle.stat();

  let buffer = Buffer.alloc(0);
  let offset = 0;

  bufferSize = Math.min((bufferSize as number) || stat.blksize || 1024, stat.size);
  const terminatorBuffer = terminator ? (Buffer.isBuffer(terminator) ? terminator : Buffer.from(terminator)) : null;

  while (offset < stat.size) {
    const dataBufferSize = Math.min(stat.size - offset, bufferSize);

    if (bufferLimit && buffer.length + dataBufferSize > bufferLimit) {
      throw new Error(`Cannot allocate additional buffer of size ${dataBufferSize} limit of ${bufferLimit} reached`);
    }

    const data = await handle.read(Buffer.alloc(dataBufferSize), 0, dataBufferSize, offset);
    offset += data.bytesRead;

    if (!terminatorBuffer) {
      yield await transform(data.buffer);
      continue;
    }

    buffer = Buffer.concat([buffer, data.buffer]);

    let terminatorIndex;
    while ((terminatorIndex = buffer.indexOf(terminatorBuffer)) > -1) {
      yield await transform(buffer.slice(0, terminatorIndex - (includeTerminator ? 0 : terminatorBuffer.byteLength)));
      buffer = buffer.slice(terminatorIndex + terminatorBuffer.byteLength);
    }
  }

  await handle.close();
}
