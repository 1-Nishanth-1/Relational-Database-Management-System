#include "BlockBuffer.h"
#include <cstdlib>
#include <cstring>
#include <iostream>

BlockBuffer::BlockBuffer(int blockNum)
{
    this->blockNum = blockNum;
}

BlockBuffer::BlockBuffer(char blockType)
{
    int ret = 0;
    switch (blockType)
    {
    case 'R':
        ret = getFreeBlock(REC);
        break;
    case 'I':
        ret = getFreeBlock(IND_INTERNAL);
        break;
    case 'L':
        ret = getFreeBlock(IND_LEAF);
        break;
    default:
        break;
    }
    this->blockNum = ret;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}
RecBuffer::RecBuffer() : BlockBuffer('R') {}

int BlockBuffer::getHeader(struct HeadInfo *head)
{
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
    {
        return ret;
    }

    memcpy(&head->pblock, bufferPtr + 4, 4);
    memcpy(&head->lblock, bufferPtr + 8, 4);
    memcpy(&head->rblock, bufferPtr + 12, 4);
    memcpy(&head->numEntries, bufferPtr + 16, 4);
    memcpy(&head->numAttrs, bufferPtr + 20, 4);
    memcpy(&head->numSlots, bufferPtr + 24, 4);

    return SUCCESS;
}

int RecBuffer::getRecord(union Attribute *rec, int slotNum)
{
    struct HeadInfo head;
    this->getHeader(&head);
    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
    {
        return ret;
    }

    if (slotNum < 0 || slotNum > BLOCK_SIZE)
    {
        E_OUTOFBOUND;
    }

    /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
    */
    int recordSize = attrCount * ATTR_SIZE;

    int offset = HEADER_SIZE + slotCount + (slotNum * recordSize);

    unsigned char *slotPointer = bufferPtr + offset;

    memcpy(rec, slotPointer, recordSize);

    return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum)
{
    struct HeadInfo head;
    this->getHeader(&head);
    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
    {
        return ret;
    }

    if (slotNum < 0 || slotNum > BLOCK_SIZE)
    {
        E_OUTOFBOUND;
    }

    /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
    */
    int recordSize = attrCount * ATTR_SIZE;

    int offset = HEADER_SIZE + slotCount + (slotNum * recordSize);

    unsigned char *slotPointer = bufferPtr + offset;

    memcpy(slotPointer, rec, recordSize);

    StaticBuffer::setDirtyBit(this->blockNum);

    return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr)
{

    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if (bufferNum == E_BLOCKNOTINBUFFER)
    {
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if (bufferNum == E_OUTOFBOUND)
        {
            return E_OUTOFBOUND;
        }

        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }
    for (int i = 0; i < BUFFER_CAPACITY; i++)
    {
        StaticBuffer::metainfo[i].timeStamp++;
    }
    StaticBuffer::metainfo[bufferNum].timeStamp = 0;

    *buffPtr = StaticBuffer::blocks[bufferNum];
    return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char *slotMap)
{
    unsigned char *bufferptr;

    int ret = loadBlockAndGetBufferPtr(&bufferptr);
    if (ret != SUCCESS)
    {
        return ret;
    }

    struct HeadInfo head;

    getHeader(&head);

    int slotCount = head.numSlots;

    unsigned char *slotMapBuffer = bufferptr + HEADER_SIZE;

    memcpy(slotMap, slotMapBuffer, slotCount);
    return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo *head)
{
    unsigned char *bufferptr;
    int ret = loadBlockAndGetBufferPtr(&bufferptr);
    if (ret != SUCCESS)
    {
        return ret;
    }
    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferptr;
    bufferHeader->blockType = head->blockType;
    bufferHeader->lblock = head->lblock;
    bufferHeader->numAttrs = head->numAttrs;
    bufferHeader->numSlots = head->numSlots;
    bufferHeader->pblock = head->pblock;
    bufferHeader->rblock = head->rblock;

    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::setBlockType(int blockType)
{
    unsigned char *bufferptr;
    int ret = loadBlockAndGetBufferPtr(&bufferptr);
    if (ret != SUCCESS)
    {
        return ret;
    }
    *((int32_t *)bufferptr) = blockType;

    StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::getFreeBlock(int blockType)
{
    int flag = 0;
    for (int i = 0; i < DISK_BLOCKS; i++)
    {
        if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK)
        {
            this->blockNum = i;
            flag = 1;
            break;
        }
    }
    if (!flag)
    {
        E_DISKFULL;
    }

    StaticBuffer::getFreeBuffer(this->blockNum);

    HeadInfo newHead;
    newHead.lblock = -1;
    newHead.numAttrs = 0;
    newHead.numEntries = 0;
    newHead.numSlots = 0;
    newHead.pblock = -1;
    newHead.rblock = -1;

    setHeader(&newHead);

    setBlockType(blockType);

    return this->blockNum;
}

int BlockBuffer::getBlockNum()
{
    return this->blockNum;
}

int RecBuffer::setSlotMap(unsigned char *slotMap)
{
    unsigned char *bufferptr;
    int ret = loadBlockAndGetBufferPtr(&bufferptr);
    if (ret != SUCCESS)
    {
        return ret;
    }

    struct HeadInfo head;
    getHeader(&head);

    int numSlots = head.numSlots;

    unsigned char *slotMapBuffer = bufferptr + HEADER_SIZE;

    memcpy(slotMapBuffer, slotMap, numSlots);

    return StaticBuffer::setDirtyBit(this->blockNum);
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrtype)
{
    double diff;
    if (attrtype == STRING)
    {
        diff = strcmp(attr1.sVal, attr2.sVal);
    }
    else
    {
        diff = attr1.nVal - attr2.nVal;
    }
    return diff > 0 ? 1 : diff == 0 ? 0
                                    : -1;
}
