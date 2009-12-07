package com.grooveshark.readfast;

import java.io.IOException;

public interface ILineSource
{
    String readLine() throws IOException;
}
