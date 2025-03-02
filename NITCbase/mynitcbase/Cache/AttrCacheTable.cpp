#include "AttrCacheTable.h"

#include <cstring>

AttrCacheEntry *AttrCacheTable::attrCache[MAX_OPEN];

int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry *attrCatBuf)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }

    for (AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next)
    {
        if (entry->attrCatEntry.offset == attrOffset)
        {
            *attrCatBuf = (entry->attrCatEntry);
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}

void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS],
                                          AttrCatEntry *attrCatEntry)
{
    strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);
    attrCatEntry->primaryFlag = record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
    strcpy(attrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal);
    attrCatEntry->attrType = record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
    attrCatEntry->rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
    attrCatEntry->offset = record[ATTRCAT_OFFSET_INDEX].nVal;
}

int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuffer)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    for (AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next)
    {
        if (strcmp(attrName, entry->attrCatEntry.attrName) == 0)
        {
            *attrCatBuffer = (entry->attrCatEntry);
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    for (AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next)
    {
        if (strcmp(entry->attrCatEntry.attrName, attrName) == 0)
        {
            searchIndex->block = entry->searchIndex.block;
            searchIndex->index = entry->searchIndex.index;
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}
int AttrCacheTable::getSearchIndex(int relId, int attrOffset, IndexId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    for (AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next)
    {
        if (entry->attrCatEntry.offset == attrOffset)
        {
            searchIndex->block = entry->searchIndex.block;
            searchIndex->index = entry->searchIndex.index;
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    for (AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next)
    {
        if (strcmp(entry->attrCatEntry.attrName, attrName) == 0)
        {
            entry->searchIndex.block = searchIndex->block;
            entry->searchIndex.index = searchIndex->index;
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}
int AttrCacheTable::setSearchIndex(int relId, int attrOffset, IndexId *searchIndex)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    for (AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next)
    {
        if (entry->attrCatEntry.offset == attrOffset)
        {
            entry->searchIndex.block = searchIndex->block;
            entry->searchIndex.index = searchIndex->index;
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::resetSearchIndex(int relId, char attrName[ATTR_SIZE])
{
    IndexId searchIndex = {-1, -1};
    return setSearchIndex(relId, attrName, &searchIndex);
}

int AttrCacheTable::resetSearchIndex(int relId, int attrOffset)
{
    IndexId searchIndex = {-1, -1};
    return setSearchIndex(relId, attrOffset, &searchIndex);
}

int AttrCacheTable::setAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuff)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    for (AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next)
    {
        if (strcmp(entry->attrCatEntry.attrName, attrName) == 0)
        {
            memcpy(&(entry->attrCatEntry), attrCatBuff, sizeof(AttrCatEntry));
            entry->dirty = true;
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}

int AttrCacheTable::setAttrCatEntry(int relId, int offset, AttrCatEntry *attrCatBuff)
{
    if (relId < 0 || relId >= MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (attrCache[relId] == nullptr)
    {
        return E_RELNOTOPEN;
    }
    for (AttrCacheEntry *entry = attrCache[relId]; entry != nullptr; entry = entry->next)
    {
        if (entry->attrCatEntry.offset == offset)
        {
            memcpy(&(entry->attrCatEntry), attrCatBuff, sizeof(AttrCatEntry));
            entry->dirty = true;
            return SUCCESS;
        }
    }
    return E_ATTRNOTEXIST;
}

void AttrCacheTable::attrCatEntryToRecord(AttrCatEntry *attrCatBuff, union Attribute record[ATTRCAT_NO_ATTRS])
{
    strcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, attrCatBuff->relName);
    record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = attrCatBuff->primaryFlag;
    strcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal, attrCatBuff->attrName);
    record[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrCatBuff->attrType;
    record[ATTRCAT_ROOT_BLOCK_INDEX].nVal = attrCatBuff->rootBlock;
    record[ATTRCAT_OFFSET_INDEX].nVal = attrCatBuff->offset;
}