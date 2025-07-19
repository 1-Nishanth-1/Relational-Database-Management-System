#include "RelCacheTable.h"
#include <stdio.h>
#include <string.h>
#include <cstring>

RelCacheEntry *RelCacheTable::relCache[MAX_OPEN];

int RelCacheTable::getRelCatEntry(int relID, RelCatEntry *relCatBuf)
{
    if (relID < 0 || relID >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (relCache[relID] == nullptr)
    {
        return E_RELNOTOPEN;
    }

    *relCatBuf = relCache[relID]->relCatEntry;
    return SUCCESS;
}

void RelCacheTable::recordToRelCatEntry(union Attribute record[RELCAT_NO_ATTRS],
                                        RelCatEntry *relCatEntry)
{
    strcpy(relCatEntry->relName, record[RELCAT_REL_NAME_INDEX].sVal);
    relCatEntry->numAttrs = (int)record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    relCatEntry->firstBlk = (int)record[RELCAT_FIRST_BLOCK_INDEX].nVal;
    relCatEntry->lastBlk = (int)record[RELCAT_LAST_BLOCK_INDEX].nVal;
    relCatEntry->numRecs = (int)record[RELCAT_NO_RECORDS_INDEX].nVal;
    relCatEntry->numSlotsPerBlk = (int)record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal;
}

int RelCacheTable::getSearchIndex(int relId, RecId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }
    if (relCache[relId] == nullptr)
    {
        return E_RELNOTEXIST;
    }
    *searchIndex = relCache[relId]->searchIndex;
    return SUCCESS;
}

int RelCacheTable::setSearchIndex(int relId, RecId *searchIndex)
{
    if (relId < 0 || relId > MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }
    if (relCache[relId] == nullptr)
    {
        E_RELNOTEXIST;
    }
    relCache[relId]->searchIndex = *searchIndex;
    return SUCCESS;
}

int RelCacheTable::resetSearchIndex(int relId)
{
    RecId resetIndex;
    resetIndex.block = -1;
    resetIndex.slot = -1;
    return setSearchIndex(relId, &resetIndex);
}

int RelCacheTable::setRelCatEntry(int relId, RelCatEntry *relCatBuffer)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }
    if (relCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    memcpy(&(RelCacheTable::relCache[relId]->relCatEntry), relCatBuffer, sizeof(RelCatEntry));
    relCache[relId]->dirty = true;
    return SUCCESS;
}

void RelCacheTable::relCatEntryToRecord(RelCatEntry *relCatEntry, union Attribute record[RELCAT_NO_ATTRS])
{
    record[RELCAT_LAST_BLOCK_INDEX].nVal = relCatEntry->lastBlk;
    record[RELCAT_NO_ATTRIBUTES_INDEX].nVal = relCatEntry->numAttrs;
    record[RELCAT_FIRST_BLOCK_INDEX].nVal = relCatEntry->firstBlk;
    record[RELCAT_NO_RECORDS_INDEX].nVal = relCatEntry->numRecs;
    record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = relCatEntry->numSlotsPerBlk;
    strcpy(record[RELCAT_REL_NAME_INDEX].sVal, relCatEntry->relName);
}