package com.grooveshark.readfast;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.FileInputStream;
import java.io.IOException;

public class ConservativeLineSource implements ILineSource
{
    private static final int DEFAULT_BUFFER_SIZE = 64*1024;      // in bytes
    private static final int DEFAULT_CHUNK_SIZE = 500*1024*1024;      // in bytes
    private static final int DEFAUL_LINE_BUFFER_SIZE = 10*1024;

    private File file;
    private FileChannel fileChannel;
    private long fileSize;
    private long chunkSize;
    private long tailChunkSize;
    private int nChunks;
    private int currentChunkNum;

    private int bufferSize;
    private int nBuffers;
    private int tailBufferSize;
    private int currentBufferNum;
    private int lineBufferSize;
    private byte[] buffer;

    private String[] lines;
    private int lastReadableLine;
    private int currentLine;

    private ByteBuffer bb;
    private String currentBufferString = "";

    private boolean eof;

    public ConservativeLineSource(File file, int bufferSize, long chunkSize) throws IOException
    {
        this.file = file;
        if (!file.exists()) {
            throw new IOException("File not found: " + file.toString());
        }
        this.fileSize = file.length();

        if (fileSize == 0) {
            this.eof = true;
            return;
        } else {
            this.eof = false;
        }

        if (fileSize < chunkSize) {
            this.chunkSize = fileSize;
        } else {
            this.chunkSize = chunkSize;
        }
        this.tailChunkSize = fileSize % this.chunkSize;
        this.nChunks = (int)(fileSize / this.chunkSize) + ((tailChunkSize > 0) ? 1 : 0);
        this.currentChunkNum = 0;

        this.bufferSize = bufferSize;
        this.buffer = new byte[bufferSize];
        this.currentBufferNum = 0;
        this.tailBufferSize = (int)(this.chunkSize % bufferSize);
        this.nBuffers = (int)(this.chunkSize / bufferSize) + ((tailBufferSize > 0) ? 1 : 0);
        this.lines = new String[DEFAUL_LINE_BUFFER_SIZE];

        this.currentLine = 0;
        this.lastReadableLine = -1;

        this.fileChannel = new FileInputStream(file).getChannel();
        this.bb = fileChannel.map(FileChannel.MapMode.READ_ONLY,0,(int)this.chunkSize);
    }

    public ConservativeLineSource(String filename, long chunkSize) throws IOException
    {
        this(new File(filename), DEFAULT_BUFFER_SIZE, chunkSize);
    }

    public ConservativeLineSource(String filename) throws IOException
    {
        this(new File(filename), DEFAULT_BUFFER_SIZE, DEFAULT_CHUNK_SIZE);
    }

    public void close()
    {
        try {
            this.fileChannel.close();
        } catch (Exception e) { }
    }

    private boolean mapNextChunk() throws IOException
    {
        currentBufferNum = 0;
        currentChunkNum++;
        if (currentChunkNum < nChunks - 1) {
            bb = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    currentChunkNum*chunkSize,
                    (int) chunkSize
                    );
            tailBufferSize = (int)(chunkSize % bufferSize);
            nBuffers = (int)(chunkSize / bufferSize) + ((tailBufferSize > 0) ? 1 : 0);
            return false;
        } else {
            bb = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    currentChunkNum*chunkSize,
                    (int) tailChunkSize);
            tailBufferSize = (int)(tailChunkSize % bufferSize);
            nBuffers = (int)(tailChunkSize / bufferSize) + ((tailBufferSize > 0) ? 1 : 0);
            return true;
        }
    }

    private int prevNewlinePos = 0;
    private int nextNewlinePos = 0;
    private boolean eom = false;
    private boolean loadNextBuffer() throws IOException
    {
        if (!eof) {
            if (lastReadableLine > 0) currentBufferString = lines[lastReadableLine];
            if (currentBufferNum < nBuffers - 1) {
                bb.get(buffer, 0, bufferSize);
                currentBufferString += new String(buffer,0,bufferSize);
                currentBufferNum++;
            } else {
                if (tailBufferSize > 0) {
                    bb.get(buffer, 0, tailBufferSize);
                    currentBufferString += new String(buffer,0,tailBufferSize);
                } else {
                    bb.get(buffer, 0, bufferSize);
                    currentBufferString += new String(buffer,0,bufferSize);
                }
                if (nChunks == 1) {
                    eof = true;
                } else {
                    if (!eom) {
                        eom = mapNextChunk();
                    } else {
                        eof = true;
                    }
                }
            }
            currentLine = 0;
            prevNewlinePos = -1;
            while ((nextNewlinePos = currentBufferString.indexOf("\n",prevNewlinePos+1)) != -1) {
                try {
                    lines[currentLine] = currentBufferString.substring(
                            prevNewlinePos+1,
                            nextNewlinePos
                            );
                } catch (Exception e) {
                    e.printStackTrace();
                }
                prevNewlinePos = nextNewlinePos;
                currentLine++;
            }
            if (currentBufferString.charAt(currentBufferString.length()-1) == '\n') {
                lines[currentLine-1] += "\n";
            } else {
                lines[currentLine++] = currentBufferString.substring(
                        prevNewlinePos+1
                        );
            }
            lastReadableLine = (eof) ? currentLine : currentLine-1;
            currentLine = 0;
            return true;
        }
        return false;
    } 

    public ConservativeLineSource( File file ) throws IOException
    {
        this(file, DEFAULT_BUFFER_SIZE, DEFAULT_CHUNK_SIZE);
    }

    public String readLine() throws IOException
    {
        if (currentLine >= lastReadableLine) {
            if (!loadNextBuffer()) {
                return null;
            }
        }
        return lines[currentLine++].trim();
    }
}
