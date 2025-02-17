#include "StaticBuffer.h"
#include <iostream>

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];

struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer()
{
    for (int i = 0; i <= 3; i++)
    {

        Disk::readBlock(&blockAllocMap[i * 2048], i);
    }

    for (int i = 0; i < BUFFER_CAPACITY; i++)
    {
        metainfo[i].free = true;
        metainfo[i].dirty = false;
        metainfo[i].timeStamp = -1;
        metainfo[i].blockNum = -1;
    }
}

StaticBuffer::~StaticBuffer()
{
    for (int i = 0; i < 3; i++)
    {
        Disk::writeBlock(&blockAllocMap[i * 2048], i);
    }
    for (int i = 0; i < BUFFER_CAPACITY; i++)
    {
        if (metainfo[i].free == false && metainfo[i].dirty == true)
        {
            Disk::writeBlock(blocks[i], metainfo[i].blockNum);
        }
    }
}

int StaticBuffer::getFreeBuffer(int blockNum)
{
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }

    for (int i = 0; i < BUFFER_CAPACITY; i++)
    {
        metainfo[i].timeStamp++;
    }

    int bufferNum = -1;
    for (int i = 0; i < BUFFER_CAPACITY; i++)
    {
        if (metainfo[i].free == true)
        {
            bufferNum = i;
            break;
        }
    }
    if (bufferNum == -1)
    {
        int index = -1;
        int maxTime = -1;
        for (int i = 0; i < BLOCK_SIZE; i++)
        {
            if (metainfo[i].timeStamp > maxTime)
            {
                maxTime = metainfo[i].timeStamp;
                index = i;
            }
            if (metainfo[index].dirty == true)
            {
                Disk::writeBlock(blocks[index], metainfo[index].blockNum);
            }
            bufferNum = index;
        }
    }
    metainfo[bufferNum].dirty = false;
    metainfo[bufferNum].timeStamp = 0;
    metainfo[bufferNum].free = false;
    metainfo[bufferNum].blockNum = blockNum;

    return bufferNum;
}

int StaticBuffer::getBufferNum(int blockNum)
{
    if (blockNum < 0 || blockNum > DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }
    for (int i = 0; i < BUFFER_CAPACITY; i++)
    {
        if (metainfo[i].blockNum == blockNum)
        {
            return i;
        }
    }
    return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum)
{
    int bufferNum = StaticBuffer::getBufferNum(blockNum);

    if (bufferNum < 0)
    {
        return bufferNum;
    }

    metainfo[bufferNum].dirty = true;
    return SUCCESS;
}