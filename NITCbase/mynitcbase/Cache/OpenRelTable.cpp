#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>
#include <iostream>
OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];
OpenRelTable::OpenRelTable()
{
    for (int i = 0; i < MAX_OPEN; i++)
    {
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
        tableMetaInfo[i].free = true;
    }

    RecBuffer relCatBlock(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];

    for (int i = 0; i < 2; i++)
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
    for (int j = 0; j < 2; j++)
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
    tableMetaInfo[RELCAT_RELID].free = false;
    tableMetaInfo[ATTRCAT_RELID].free = false;
    strcpy(tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
    strcpy(tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);
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
    for (int i = 2; i < MAX_OPEN; ++i)
    {
        if (!tableMetaInfo[i].free)
        {
            OpenRelTable::closeRel(i);
        }
    }

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
    for (int i = 0; i < MAX_OPEN; i++)
    {
        if (!strcmp(relName, tableMetaInfo[i].relName))
        {
            return i;
        }
    }

    return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry()
{
    for (int i = 3; i < MAX_OPEN; i++)
    {
        if (tableMetaInfo[i].free == true)
        {
            return i;
        }
    }
    return E_CACHEFULL;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE])
{
    int res = OpenRelTable::getRelId(relName);
    if (res > 0)
    {
        return res;
    }

    int relId = OpenRelTable::getFreeOpenRelTableEntry();

    if (relId == E_CACHEFULL)
    {
        return E_CACHEFULL;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute attrRelName;
    strcpy(attrRelName.sVal, relName);

    RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, attrRelName, EQ);

    if (relcatRecId.block == -1 && relcatRecId.slot == -1)
    {
        return E_RELNOTEXIST;
    }

    RecBuffer relationBuffer(relcatRecId.block);
    Attribute relRecord[RELCAT_NO_ATTRS];
    relationBuffer.getRecord(relRecord, relcatRecId.slot);

    RelCacheEntry *relCacheEntry = (RelCacheEntry *)malloc(sizeof(RelCacheEntry));
    RelCacheTable::recordToRelCatEntry(relRecord, &relCacheEntry->relCatEntry);

    relCacheEntry->recId = relcatRecId;
    RelCacheTable::relCache[relId] = relCacheEntry;

    // inserting attributes

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    Attribute attrRecord[ATTRCAT_NO_ATTRS];

    AttrCacheEntry *listHead = nullptr;
    AttrCacheEntry *current = nullptr;

    int numberOfAttributes = RelCacheTable::relCache[relId]->relCatEntry.numAttrs;

    for (int i = 0; i < numberOfAttributes; i++)
    {

        RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, (char *)RELCAT_ATTR_RELNAME, attrRelName, EQ);

        RecBuffer attrCatBlock = RecBuffer(attrcatRecId.block);
        attrCatBlock.getRecord(attrRecord, attrcatRecId.slot);
        AttrCacheEntry *attrCacheEntry = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
        AttrCacheTable::recordToAttrCatEntry(attrRecord, &(attrCacheEntry->attrCatEntry));
        attrCacheEntry->recId = attrcatRecId;

        if (current != nullptr)
        {
            current->next = attrCacheEntry;
        }
        else
        {
            listHead = attrCacheEntry;
        }

        current = attrCacheEntry;
    }

    current->next = nullptr;

    AttrCacheTable::attrCache[relId] = listHead;
    listHead = nullptr;
    current = nullptr;

    tableMetaInfo[relId].free = false;
    strcpy(tableMetaInfo[relId].relName, relName);

    return SUCCESS;
}

int OpenRelTable::closeRel(int relId)
{

    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
    {
        return E_NOTPERMITTED;
    }

    if (relId < 0 || relId > MAX_OPEN)
    {
        return E_OUTOFBOUND;
    }

    if (tableMetaInfo[relId].free == true)
    {
        return E_RELNOTOPEN;
    }

    AttrCacheEntry *current = AttrCacheTable::attrCache[relId];
    while (current != nullptr)
    {
        AttrCacheEntry *next = current->next;
        free(current);
        current = next;
    }
    AttrCacheTable::attrCache[relId] = nullptr;

    if (RelCacheTable::relCache[relId] != nullptr)
    {
        free(RelCacheTable::relCache[relId]);
        RelCacheTable::relCache[relId] = nullptr;
    }

    tableMetaInfo[relId].free = true;
    strcpy(tableMetaInfo[relId].relName, "");
    return SUCCESS;
}