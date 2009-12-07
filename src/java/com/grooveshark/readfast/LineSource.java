package com.grooveshark.readfast;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.FileInputStream;
import java.io.IOException;

public class LineSource implements ILineSource
{
    private static final int DEFAULT_BUFFER_SIZE = 16*1024;      // in bytes

    private File file;
    private FileChannel fileChannel;
    private long fileSize;

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

    public LineSource(File file, int bufferSize) throws IOException
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

        this.bufferSize = bufferSize;
        this.buffer = new byte[bufferSize];
        this.currentBufferNum = 0;
        this.tailBufferSize = (int)fileSize % bufferSize;
        this.nBuffers = (int)fileSize / bufferSize + ((tailBufferSize > 0) ? 1 : 0);
        this.lineBufferSize = 1000;
        this.lines = new String[lineBufferSize];

        this.currentLine = 0;
        this.lastReadableLine = -1;

        this.fileChannel = new FileInputStream(file).getChannel();
        this.bb = fileChannel.map(FileChannel.MapMode.READ_ONLY,0,(int)fileSize);
    }

    public LineSource(String filename, int bufferSize) throws IOException
    {
        this(new File(filename), bufferSize);
    }

    public LineSource(String filename) throws IOException
    {
        this(new File(filename), DEFAULT_BUFFER_SIZE);
    }

    public void close()
    {
        try {
            this.fileChannel.close();
        } catch (Exception e) { }
    }

    private int prevNewlinePos = 0;
    private int nextNewlinePos = 0;
    private boolean loadNextBuffer()
    {
        if (!eof) {
            if (lastReadableLine > 0) currentBufferString = lines[lastReadableLine];
            if (currentBufferNum < nBuffers - 1) {
                bb.get(buffer, 0, bufferSize);
                currentBufferString += new String(buffer,0,bufferSize);
                currentBufferNum++;
            } else {
                bb.get(buffer, 0, tailBufferSize);
                currentBufferString += new String(buffer,0,tailBufferSize);
                eof = true;
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

    public LineSource( File file ) throws IOException
    {
        this(file, DEFAULT_BUFFER_SIZE);
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
