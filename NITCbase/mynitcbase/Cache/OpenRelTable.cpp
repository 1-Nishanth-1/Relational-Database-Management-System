#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>
#include <iostream>
OpenRelTable::OpenRelTable()
{
    for (int i = 0; i < MAX_OPEN; i++)
    {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
    }

    RecBuffer relCatBlock(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    HeadInfo RelCatHeader;
    relCatBlock.getHeader(&RelCatHeader);

    for (int i = 0; i < RelCatHeader.numEntries; i++)
    {
        relCatBlock.getRecord(relCatRecord, i);

        struct RelCacheEntry relCacheEntry;
        RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
        relCacheEntry.recId.block = RELCAT_BLOCK;
        relCacheEntry.recId.slot = i;

        RelCacheTable::relCache[i] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
        *(RelCacheTable::relCache[i]) = relCacheEntry;
    }

    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
    AttrCacheEntry *head = nullptr;
    AttrCacheEntry *current = nullptr;
    int attrCount = 0;
    int slotNum = 0;
    int i = 0;
    for (int j = 0; j < RelCatHeader.numEntries; j++)
    {
        HeadInfo attrCatHeader;
        attrCatBuffer.getHeader(&attrCatHeader);
        int noOfAttributes = RelCacheTable::relCache[j]->relCatEntry.numAttrs;

        for (; i < noOfAttributes + attrCount; i++, slotNum++)
        {

            attrCatBuffer.getRecord(attrCatRecord, slotNum);
            AttrCacheEntry *attrCacheEntry = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
            AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
            attrCacheEntry->recId.block = ATTRCAT_BLOCK + int(i / 20);
            attrCacheEntry->recId.slot = slotNum;

            attrCacheEntry->next = nullptr;

            if (current != nullptr)
            {
                current->next = attrCacheEntry;
            }
            else
            {
                head = attrCacheEntry;
            }

            current = attrCacheEntry;
            if (slotNum + 1 == attrCatHeader.numSlots)
            {
                slotNum = -1;
                attrCatBuffer = RecBuffer(attrCatHeader.rblock);
                attrCatBuffer.getHeader(&attrCatHeader);
            }
        }

        attrCount += noOfAttributes;
        current->next = nullptr;

        AttrCacheTable::attrCache[j] = head;
        head = nullptr;
        current = nullptr;
    }
}

// head = nullptr;
// current = nullptr;

//     for (int i = 6; i < 12; ++i)
//     {
//         attrCatBuffer.getRecord(attrCatRecord, i);

//         AttrCacheEntry *attrCacheEntry = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));

//         AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));

//         attrCacheEntry->recId.block = ATTRCAT_BLOCK;
//         attrCacheEntry->recId.slot = i;

//         attrCacheEntry->next = nullptr;

//         if (current != nullptr)
//         {
//             current->next = attrCacheEntry;
//         }
//         else
//         {
//             head = attrCacheEntry;
//         }

//         current = attrCacheEntry;
//     }

//     current->next = nullptr;

//     AttrCacheTable::attrCache[ATTRCAT_RELID] = head;
// }

OpenRelTable::~OpenRelTable()
{
    for (int i = 0; i < MAX_OPEN; ++i)
    {
        if (RelCacheTable::relCache[i] != nullptr)
        {
            free(RelCacheTable::relCache[i]);
            RelCacheTable::relCache[i] = nullptr;
        }
    }

    for (int i = 0; i < MAX_OPEN; ++i)
    {
        AttrCacheEntry *current = AttrCacheTable::attrCache[i];
        while (current != nullptr)
        {
            AttrCacheEntry *next = current->next;
            free(current);
            current = next;
        }
        AttrCacheTable::attrCache[i] = nullptr;
    }
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE])
{

    if (!strcmp(relName, RELCAT_RELNAME))
    {
        return RELCAT_RELID;
    }

    if (!strcmp(relName, ATTRCAT_RELNAME))
    {
        return ATTRCAT_RELID;
    }

    return E_RELNOTOPEN;
}