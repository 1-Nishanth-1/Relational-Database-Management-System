#include "BlockBuffer.h"
#include <cstdlib>
#include <cstring>

BlockBuffer::BlockBuffer(int blockNum)
{
    this->blockNum = blockNum;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

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

    unsigned char buffer[BLOCK_SIZE];

    Disk::readBlock(buffer, this->blockNum);

    /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
    */
    int recordSize = attrCount * ATTR_SIZE;

    int offset = HEADER_SIZE + slotCount + (slotNum * recordSize);

    unsigned char *slotPointer = buffer + offset;

    memcpy(slotPointer, rec, recordSize);

    Disk::writeBlock(buffer, this->blockNum);

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

    unsigned char *slotMapInBuffer = bufferptr + HEADER_SIZE;

    memcpy(slotMap, slotMapInBuffer, slotCount);
    return SUCCESS;
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