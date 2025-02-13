#include "BlockAccess.h"

#include <cstring>
#include <iostream>
RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    int block = prevRecId.block;
    int slot = prevRecId.slot;

    RelCatEntry relCatEntry;

    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);
        block = relCatEntry.firstBlk;
        slot = 0;
    }
    else
    {
        slot += 1;
    }
    while (block != -1)
    {
        RecBuffer relCatBuffer(block);
        HeadInfo relHeader;
        Attribute relRecord[RELCAT_NO_ATTRS];

        relCatBuffer.getRecord(relRecord, slot);
        relCatBuffer.getHeader(&relHeader);
        unsigned char slotMap[relHeader.numSlots];
        relCatBuffer.getSlotMap(slotMap);

        if (slot >= relHeader.numSlots)
        {
            block = relHeader.rblock;
            slot = 0;
            continue;
        }
        if (slotMap[slot] == SLOT_UNOCCUPIED)
        {
            slot++;
            continue;
        }
        AttrCatEntry attrCatBuffer;
        AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuffer);

        Attribute attrRecord[relHeader.numAttrs];

        relCatBuffer.getRecord(attrRecord, slot);

        int attrOffset = attrCatBuffer.offset;

        int cmpVal = compareAttrs(attrRecord[attrOffset], attrVal, attrCatBuffer.attrType);

        if (
            (op == NE && cmpVal != 0) ||
            (op == LT && cmpVal < 0) ||
            (op == LE && cmpVal <= 0) ||
            (op == EQ && cmpVal == 0) ||
            (op == GT && cmpVal > 0) ||
            (op == GE && cmpVal >= 0))
        {
            RecId newRecId;
            newRecId.block = block;
            newRecId.slot = slot;
            RelCacheTable::setSearchIndex(relId, &newRecId);
            return newRecId;
        }
        slot++;
    }
    return RecId{-1, -1};
}
int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute newRelationName;
    strcpy(newRelationName.sVal, newName);
    RecId recordId = BlockAccess::linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, newRelationName, EQ);

    if (recordId.block != -1 && recordId.slot != -1)
    {
        return E_RELEXIST;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute oldRelationName;
    strcpy(oldRelationName.sVal, oldName);
    recordId = BlockAccess::linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, oldRelationName, EQ);
    if (recordId.block == -1 && recordId.slot == -1)
    {
        return E_RELNOTEXIST;
    }

    Attribute relCatRecord[RELCAT_NO_ATTRS];
    RecBuffer relCatBuffer(RELCAT_BLOCK);
    relCatBuffer.getRecord(relCatRecord, recordId.slot);
    relCatRecord[RELCAT_REL_NAME_INDEX] = newRelationName;
    relCatBuffer.setRecord(relCatRecord, recordId.slot);

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    for (int i = 0; i < relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal; i++)
    {
        RecId attrRecordId = BlockAccess::linearSearch(ATTRCAT_RELID, (char *)ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);

        RecBuffer attrCatBuffer(attrRecordId.block);
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBuffer.getRecord(attrCatRecord, attrRecordId.slot);
        attrCatRecord[ATTRCAT_REL_NAME_INDEX] = newRelationName;
        attrCatBuffer.setRecord(attrCatRecord, attrRecordId.slot);
    }
    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);
    RecId searchIndex = linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, relNameAttr, EQ);
    if (searchIndex.block == -1 && searchIndex.slot == -1)
    {
        return E_RELNOTEXIST;
    }
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    RecId attrToRenameRecId;
    attrToRenameRecId.block = -1;
    attrToRenameRecId.slot = -1;
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    while (true)
    {
        searchIndex = BlockAccess::linearSearch(ATTRCAT_RELID, (char *)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
        if (searchIndex.block == -1 && searchIndex.slot == -1)
        {
            break;
        }

        RecBuffer attrBuffer(searchIndex.block);
        Attribute attrRecord[ATTRCAT_NO_ATTRS];

        attrBuffer.getRecord(attrRecord, searchIndex.slot);

        if (strcmp(attrRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0)
        {
            attrToRenameRecId = searchIndex;
            break;
        }
        if (strcmp(attrRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0)
        {
            return E_ATTREXIST;
        }
    }
    if (attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1)
    {
        return E_ATTRNOTEXIST;
    }

    RecBuffer attrBuffer(attrToRenameRecId.block);
    Attribute attrRecord[ATTRCAT_NO_ATTRS];
    attrBuffer.getRecord(attrRecord, attrToRenameRecId.slot);
    strcpy(attrRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
    attrBuffer.setRecord(attrRecord, attrToRenameRecId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record)
{
    RelCatEntry relCatBuffer;
    RelCacheTable::getRelCatEntry(relId, &relCatBuffer);

    int blockNum = relCatBuffer.firstBlk;
    RecId recId = {-1, -1};

    int numOfSlots = relCatBuffer.numSlotsPerBlk;
    int numOfAttributes = relCatBuffer.numAttrs;
    int prevBlockNum = -1;

    while (blockNum != -1)
    {
        RecBuffer recBuffer(blockNum);
        HeadInfo recHeader;
        recBuffer.getHeader(&recHeader);
        unsigned char slotMap[recHeader.numSlots];
        recBuffer.getSlotMap(slotMap);
        for (int i = 0; i < recHeader.numSlots; i++)
        {
            if (slotMap[i] == SLOT_UNOCCUPIED)
            {
                recId.block = blockNum;
                recId.slot = i;
                break;
            }
        }
        if (recId.block != -1 && recId.slot != -1)
        {
            break;
        }
        prevBlockNum = blockNum;
        blockNum = recHeader.rblock;
    }
    if (recId.block == -1 && recId.slot == -1)
    {
        if (relId == RELCAT_RELID)
        {
            return E_MAXRELATIONS;
        }
        RecBuffer recBuffer;
        int ret = recBuffer.getBlockNum();
        if (ret == E_DISKFULL)
        {
            return E_DISKFULL;
        }
        recId.block = ret;
        recId.slot = 0;

        HeadInfo newHead;
        newHead.blockType = REC;
        newHead.pblock = -1;
        newHead.lblock = prevBlockNum;
        newHead.rblock = -1;
        newHead.numEntries = 0;
        newHead.numEntries = 0;
        newHead.numSlots = numOfSlots;
        newHead.numAttrs = numOfAttributes;
        recBuffer.setHeader(&newHead);
        unsigned char newSlotmap[numOfSlots];
        memset(newSlotmap, SLOT_UNOCCUPIED, numOfSlots);
        recBuffer.setSlotMap(newSlotmap);

        if (prevBlockNum != -1)
        {
            RecBuffer prevRecBuffer(prevBlockNum);
            HeadInfo prevHeader;
            prevRecBuffer.getHeader(&prevHeader);
            prevHeader.rblock = recId.block;
            prevRecBuffer.setHeader(&prevHeader);
        }
        else
        {
            relCatBuffer.firstBlk = recId.block;
            std::cout << "RelCatEntry values:" << std::endl;
            std::cout << "firstBlk: " << relCatBuffer.firstBlk << std::endl;
            std::cout << "lastBlk: " << relCatBuffer.lastBlk << std::endl;
            std::cout << "numSlotsPerBlk: " << relCatBuffer.numSlotsPerBlk << std::endl;
            std::cout << "numAttrs: " << relCatBuffer.numAttrs << std::endl;
            std::cout << "numRecs: " << relCatBuffer.numRecs << std::endl;
            RelCacheTable::setRelCatEntry(relId, &relCatBuffer);
            std::cout << "RelCatEntry values:" << std::endl;
        }
        relCatBuffer.lastBlk = recId.block;
        RelCacheTable::setRelCatEntry(relId, &relCatBuffer);
    }
    RecBuffer newrecBuffer(recId.block);
    newrecBuffer.setRecord(record, recId.slot);

    unsigned char slotmap[numOfSlots];
    newrecBuffer.getSlotMap(slotmap);
    slotmap[recId.slot] = SLOT_OCCUPIED;
    newrecBuffer.setSlotMap(slotmap);
    HeadInfo header;
    newrecBuffer.getHeader(&header);
    header.numEntries = header.numEntries + 1;
    newrecBuffer.setHeader(&header);
    relCatBuffer.numRecs = relCatBuffer.numRecs + 1;
    RelCacheTable::setRelCatEntry(relId, &relCatBuffer);
    return SUCCESS;
}