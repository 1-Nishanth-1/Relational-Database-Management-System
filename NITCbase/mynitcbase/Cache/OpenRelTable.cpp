#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>

OpenRelTable::OpenRelTable()
{
    for (int i = 0; i < MAX_OPEN; i++)
    {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
    }

    RecBuffer relCatBlock(RELCAT_BLOCK);

    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

    struct RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

    RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

    RecBuffer attrCatBlock(ATTRCAT_BLOCK);

    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

    relCatBlock.getRecord(attrCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);

    struct RelCacheEntry attrCacheEntry;
    RelCacheTable::recordToRelCatEntry(attrCatRecord, &attrCacheEntry.relCatEntry);
    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
    attrCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

    RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry *)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[ATTRCAT_RELID]) = attrCacheEntry;

    RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
    AttrCacheEntry *head = nullptr;
    AttrCacheEntry *current = nullptr;

    for (int i = 0; i < 6; ++i)
    {
        attrCatBuffer.getRecord(attrCatRecord, i);
        AttrCacheEntry *attrCacheEntry = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
        attrCacheEntry->recId.block = ATTRCAT_BLOCK;
        attrCacheEntry->recId.slot = i;

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
    }

    current->next = nullptr;

    AttrCacheTable::attrCache[RELCAT_RELID] = head;

    head = nullptr;
    current = nullptr;

    for (int i = 6; i < 12; ++i)
    {
        attrCatBuffer.getRecord(attrCatRecord, i);

        AttrCacheEntry *attrCacheEntry = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));

        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));

        attrCacheEntry->recId.block = ATTRCAT_BLOCK;
        attrCacheEntry->recId.slot = i;

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
    }

    current->next = nullptr;

    AttrCacheTable::attrCache[ATTRCAT_RELID] = head;
}

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
