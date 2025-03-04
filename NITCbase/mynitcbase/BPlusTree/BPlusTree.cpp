#include "BPlusTree.h"
#include <iostream>
#include <cstring>
RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
    IndexId searchIndex;
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    int block, index;
    if (searchIndex.block == -1 && searchIndex.index == -1)
    {
        block = attrCatEntry.rootBlock;
        index = 0;
        if (block == -1)
        {
            return RecId{-1, -1};
        }
    }
    else
    {
        block = searchIndex.block;
        index = searchIndex.index + 1;

        IndLeaf leaf(block);
        HeadInfo leafHead;
        leaf.getHeader(&leafHead);
        if (index >= leafHead.numEntries)
        {
            block = leafHead.rblock;
            index = 0;
            if (block == -1)
                return RecId{-1, -1};
        }
    }

    while (StaticBuffer::getStaticBlockType(block) == IND_INTERNAL)
    {
        IndInternal internalBlk(block);
        HeadInfo intHead;
        internalBlk.getHeader(&intHead);
        InternalEntry intEntry;

        if (op == NE || op == LT || op == LE)
        {
            internalBlk.getEntry(&intEntry, 0);
            block = intEntry.lChild;
        }
        else
        {
            bool found = false;
            int i = 0;
            while (i < intHead.numEntries)
            {
                internalBlk.getEntry(&intEntry, i);
                int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType);
                if ((op == EQ && cmpVal >= 0) || (op == GE && cmpVal >= 0) || (op == GT && cmpVal > 0))
                {
                    found = true;
                    break;
                }
                i++;
            }
            if (found)
            {
                block = intEntry.lChild;
            }
            else
            {
                internalBlk.getEntry(&intEntry, intHead.numEntries - 1);
                block = intEntry.rChild;
            }
        }
        index = 0;
    }

    while (block != -1)
    {
        IndLeaf leafBlk(block);
        HeadInfo leafHead;
        leafBlk.getHeader(&leafHead);
        Index leafEntry;
        while (index < leafHead.numEntries)
        {
            leafBlk.getEntry(&leafEntry, index);
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType);

            if ((op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0))
            {
                IndexId newSearchIndex = {block, index};
                AttrCacheTable::setSearchIndex(relId, attrName, &newSearchIndex);
                return RecId{leafEntry.block, leafEntry.slot};
            }
            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0)
            {
                return RecId{-1, -1};
            }
            index++;
        }
        if (op != NE)
        {
            break;
        }
        block = leafHead.rblock;
        index = 0;
    }
    return RecId{-1, -1};
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE])
{
    if (relId == RELCAT_BLOCK || relId == ATTRCAT_BLOCK)
    {
        return E_NOTPERMITTED;
    }
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret < 0)
    {
        return ret;
    }
    if (attrCatEntry.rootBlock != -1)
    {
        return SUCCESS;
    }
    IndLeaf rootBlockBuf;
    int rootBlock = rootBlockBuf.getBlockNum();
    attrCatEntry.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
    if (rootBlock == E_DISKFULL)
    {
        return E_DISKFULL;
    }
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    int block = relCatEntry.firstBlk;
    while (block != -1)
    {
        RecBuffer recBuffer(block);
        unsigned char slotMap[relCatEntry.numSlotsPerBlk];
        recBuffer.getSlotMap(slotMap);
        for (int i = 0; i < relCatEntry.numSlotsPerBlk; i++)
        {
            if (slotMap[i] == SLOT_OCCUPIED)
            {
                Attribute record[relCatEntry.numAttrs];
                recBuffer.getRecord(record, i);
                RecId recId = {block, i};
                int retVal = bPlusInsert(relId, attrName, record[attrCatEntry.offset], recId);
                if (retVal == E_DISKFULL)
                {
                    return E_DISKFULL;
                }
            }
        }
        HeadInfo head;
        recBuffer.getHeader(&head);
        block = head.rblock;
    }
    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum)
{
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }
    int type = StaticBuffer::getStaticBlockType(rootBlockNum);
    if (type == IND_LEAF)
    {
        IndLeaf leaf(rootBlockNum);
        leaf.releaseBlock();
        return SUCCESS;
    }
    else if (type == IND_INTERNAL)
    {
        IndInternal internal(rootBlockNum);
        HeadInfo head;
        internal.getHeader(&head);
        InternalEntry entry;
        internal.getEntry(&entry, 0);
        bPlusDestroy(entry.lChild);
        for (int i = 0; i < head.numEntries; i++)
        {
            InternalEntry entry;
            internal.getEntry(&entry, i);
            bPlusDestroy(entry.rChild);
        }
        internal.releaseBlock();
        return SUCCESS;
    }
    else
    {
        return E_INVALIDBLOCK;
    }
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId)
{
    printf("%d\n", relId);
    printf("%d %d\n", recId.block, recId.slot);
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret < 0)
    {
        return ret;
    }
    if (attrCatEntry.rootBlock == -1)
    {
        return E_NOINDEX;
    }
    int leafBlockNum = BPlusTree::findLeafToInsert(attrCatEntry.rootBlock, attrVal, attrCatEntry.attrType);

    Index leafEntry;
    leafEntry.attrVal = attrVal;
    leafEntry.block = recId.block;
    leafEntry.slot = recId.slot;

    ret = BPlusTree::insertIntoLeaf(relId, attrName, leafBlockNum, leafEntry);
    if (ret == E_DISKFULL)
    {
        bPlusDestroy(attrCatEntry.rootBlock);
        attrCatEntry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
        return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType)
{
    int blockNum = rootBlock;

    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF)
    {
        IndInternal internalBlock(blockNum);

        HeadInfo blockHeader;
        internalBlock.getHeader(&blockHeader);

        int index = 0;
        while (index < blockHeader.numEntries)
        {
            InternalEntry entry;
            internalBlock.getEntry(&entry, index);
            if (compareAttrs(attrVal, entry.attrVal, attrType) <= 0)
                break;

            index++;
        }

        if (index == blockHeader.numEntries)
        {

            InternalEntry entry;
            internalBlock.getEntry(&entry, blockHeader.numEntries - 1);
            blockNum = entry.rChild;
        }
        else
        {
            InternalEntry entry;
            internalBlock.getEntry(&entry, index);
            blockNum = entry.lChild;
        }
    }
    return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry)
{
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    IndLeaf leafBlock(blockNum);
    HeadInfo blockHeader;
    leafBlock.getHeader(&blockHeader);
    Index indices[blockHeader.numEntries + 1];

    bool inserted = false;
    for (int i = 0; i < blockHeader.numEntries; i++)
    {
        Index temp;
        leafBlock.getEntry(&temp, i);
        if (compareAttrs(indexEntry.attrVal, temp.attrVal, attrCatEntry.attrType) <= 0)
        {
            indices[i] = indexEntry;
            for (int j = i; j < blockHeader.numEntries; j++)
            {
                leafBlock.getEntry(&indices[j + 1], j);
            }
            inserted = true;
            break;
        }
        indices[i] = temp;
    }
    if (!inserted)
    {
        indices[blockHeader.numEntries] = indexEntry;
    }

    if (blockHeader.numEntries != MAX_KEYS_LEAF)
    {
        blockHeader.numEntries++;
        leafBlock.setHeader(&blockHeader);
        for (int i = 0; i < blockHeader.numEntries; i++)
        {
            leafBlock.setEntry(&indices[i], i);
        }
        return SUCCESS;
    }

    int newRightBlk = splitLeaf(blockNum, indices);
    if (newRightBlk == E_DISKFULL)
    {
        return E_DISKFULL;
    }

    if (blockHeader.pblock != -1)
    {
        InternalEntry newEntry;
        newEntry.lChild = blockNum;
        newEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        newEntry.rChild = newRightBlk;

        int ret = BPlusTree::insertIntoInternal(relId, attrName, blockHeader.pblock, newEntry);
        if (ret == E_DISKFULL)
        {
            return E_DISKFULL;
        }
    }
    else
    {
        int ret = createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlk);
        if (ret == E_DISKFULL)
        {
            return E_DISKFULL;
        }
    }
    return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[])
{
    IndLeaf rightBlk;
    IndLeaf leftBlk(leafBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = leafBlockNum;

    if (rightBlkNum == E_DISKFULL)
    {
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;

    rightBlk.getHeader(&rightBlkHeader);
    leftBlk.getHeader(&leftBlkHeader);

    rightBlkHeader.blockType = leftBlkHeader.blockType;
    rightBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlkHeader.rblock = leftBlkHeader.rblock;
    rightBlkHeader.lblock = leftBlkNum;
    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.setHeader(&leftBlkHeader);

    for (int i = 0; i < (MAX_KEYS_LEAF + 1) / 2; i++)
    {
        leftBlk.setEntry(&indices[i], i);
    }

    for (int i = 0; i < rightBlkHeader.numEntries; i++)
    {
        rightBlk.setEntry(&indices[i + (MAX_KEYS_LEAF + 1) / 2], i);
    }

    return rightBlkNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry)
{
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    IndInternal intBlock(intBlockNum);
    HeadInfo blockHeader;
    intBlock.getHeader(&blockHeader);
    InternalEntry internalEntries[blockHeader.numEntries + 1];

    int inserted = -1;
    for (int i = 0; i < blockHeader.numEntries; i++)
    {
        InternalEntry temp;
        intBlock.getEntry(&temp, i);
        if (compareAttrs(intEntry.attrVal, temp.attrVal, attrCatEntry.attrType) < 0)
        {
            internalEntries[i] = intEntry;
            inserted = i;
            for (int j = i; j < blockHeader.numEntries; j++)
            {
                intBlock.getEntry(&internalEntries[j + 1], j);
            }
            break;
        }
        internalEntries[i] = temp;
    }
    if (inserted == -1)
    {
        internalEntries[blockHeader.numEntries] = intEntry;
    }
    else
    {
        internalEntries[inserted + 1].lChild = intEntry.rChild;
    }

    if (blockHeader.numEntries != MAX_KEYS_INTERNAL)
    {
        blockHeader.numEntries++;
        intBlock.setHeader(&blockHeader);
        for (int i = 0; i < blockHeader.numEntries; i++)
        {
            intBlock.setEntry(&internalEntries[i], i);
        }
        return SUCCESS;
    }

    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    if (newRightBlk == E_DISKFULL)
    {
        bPlusDestroy(intEntry.rChild);
        // why?
        return E_DISKFULL;
    }

    if (blockHeader.pblock != -1)
    {
        InternalEntry newEntry;
        newEntry.lChild = intBlockNum;
        newEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        newEntry.rChild = newRightBlk;
        int ret = insertIntoInternal(relId, attrName, blockHeader.pblock, newEntry);
        if (ret == E_DISKFULL)
        {
            return E_DISKFULL;
        }
    }
    else
    {
        int ret = createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlk);
        if (ret == E_DISKFULL)
        {
            return E_DISKFULL;
        }
    }
    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[])
{
    IndInternal rightBlk;
    IndInternal leftBlk(intBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = intBlockNum;

    if (rightBlkNum == E_DISKFULL)
    {
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;

    rightBlk.getHeader(&rightBlkHeader);
    leftBlk.getHeader(&leftBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    // doubt
    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.setHeader(&leftBlkHeader);

    for (int i = 0; i < (MAX_KEYS_INTERNAL) / 2; i++)
    {
        leftBlk.setEntry(&internalEntries[i], i);
    }

    for (int i = 0; i < rightBlkHeader.numEntries; i++)
    {
        rightBlk.setEntry(&internalEntries[i + (MAX_KEYS_INTERNAL + 1) / 2 + 1], i);
    }

    InternalEntry temp;
    rightBlk.getEntry(&temp, 0);
    int type = StaticBuffer::getStaticBlockType(temp.lChild);

    for (int i = 0; i < rightBlkHeader.numEntries; i++)
    {
        rightBlk.getEntry(&temp, i);
        BlockBuffer buffer(temp.lChild);
        HeadInfo head;
        buffer.getHeader(&head);
        head.pblock = rightBlkNum;
        buffer.setHeader(&head);
    }
    rightBlk.getEntry(&temp, rightBlkHeader.numEntries - 1);
    BlockBuffer buffer(temp.rChild);
    HeadInfo head;
    buffer.getHeader(&head);
    head.pblock = rightBlkNum;
    buffer.setHeader(&head);
    return rightBlkNum;
}

int BPlusTree::createNewRoot(int relId, char attrNmae[ATTR_SIZE], Attribute attrVal, int lChild, int rChild)
{
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrNmae, &attrCatEntry);
    IndInternal newRootBlock;
    int newRootBlockNum = newRootBlock.getBlockNum();
    if (newRootBlockNum == E_DISKFULL)
    {
        BPlusTree::bPlusDestroy(rChild);
        return E_DISKFULL;
    }
    HeadInfo head;
    newRootBlock.getHeader(&head);
    head.numEntries = 1;

    InternalEntry entry;
    entry.lChild = lChild;
    entry.attrVal = attrVal;
    entry.rChild = rChild;
    newRootBlock.setEntry(&entry, 0);

    BlockBuffer lChildBuffer(lChild);
    lChildBuffer.getHeader(&head);
    head.pblock = newRootBlockNum;
    lChildBuffer.setHeader(&head);

    BlockBuffer rChildBuffer(rChild);
    rChildBuffer.getHeader(&head);
    head.pblock = newRootBlockNum;
    rChildBuffer.setHeader(&head);

    attrCatEntry.rootBlock = newRootBlockNum;
    AttrCacheTable::setAttrCatEntry(relId, attrNmae, &attrCatEntry);

    return SUCCESS;
}