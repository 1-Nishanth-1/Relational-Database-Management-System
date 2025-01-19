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