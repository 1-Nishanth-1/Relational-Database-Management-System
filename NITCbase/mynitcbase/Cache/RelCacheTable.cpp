#include "RelCacheTable.h"

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