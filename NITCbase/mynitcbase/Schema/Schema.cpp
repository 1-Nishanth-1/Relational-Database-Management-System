#include "Schema.h"
#include "iostream"
#include <cmath>
#include <cstring>
#include <vector>
#include <set>

int Schema::openRel(char relName[ATTR_SIZE])
{

    int ret = OpenRelTable::openRel(relName);

    if (ret >= 0)
    {
        return SUCCESS;
    }

    return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE])
{
    if (relName == RELCAT_RELNAME || relName == ATTRCAT_RELNAME)
    {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);

    if (relId == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }

    return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE])
{
    if ((!strcmp(oldRelName, ATTRCAT_RELNAME)) || (!strcmp(oldRelName, RELCAT_RELNAME) || (!strcmp(newRelName, ATTRCAT_RELNAME)) || (!strcmp(newRelName, RELCAT_RELNAME))))
    {
        return E_NOTPERMITTED;
    }

    if (OpenRelTable::getRelId(oldRelName) != E_RELNOTOPEN)
    {
        return E_RELOPEN;
    }
    return BlockAccess::renameRelation(oldRelName, newRelName);
}

int Schema::renameAttr(char *relName, char *oldAttrName, char *newAttrName)
{
    if ((!strcmp(relName, ATTRCAT_RELNAME)) || (!strcmp(relName, RELCAT_RELNAME)))
    {
        return E_NOTPERMITTED;
    }

    if (OpenRelTable::getRelId(relName) != E_RELNOTOPEN)
    {
        return E_RELOPEN;
    }

    return BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
}

int Schema::createRel(char relName[], int nAttrs, char attr[][ATTR_SIZE], int typeAttrs[])
{
    Attribute relNameAsAttribute;
    strcpy(relNameAsAttribute.sVal, relName);
    RecId tagetRelId;
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    tagetRelId = BlockAccess::linearSearch(RELCAT_RELID, (char *)RELCAT_ATTR_RELNAME, relNameAsAttribute, EQ);
    if (tagetRelId.block != -1 && tagetRelId.slot != -1)
    {
        return E_RELEXIST;
    }
    std::set<std::string> seenAttrs;
    for (int i = 0; i < nAttrs; i++)
    {
        if (seenAttrs.find(attr[i]) != seenAttrs.end())
        {
            return E_DUPLICATEATTR;
        }
        seenAttrs.insert(attr[i]);
    }
    Attribute relCatRecord[RELCAT_NO_ATTRS];

    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
    relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
    relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
    relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor(2016 / (16 * nAttrs + 1));

    int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);
    if (retVal < 0)
    {
        return retVal;
    }
    for (int i = 0; i < nAttrs; i++)
    {
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attr[i]);
        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
        attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = typeAttrs[i];
        attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;
        retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
        if (retVal < 0)
        {
            Schema::deleteRel(relName);
            return retVal;
        }
    }
    return SUCCESS;
}

int Schema::deleteRel(char *relName)
{
    if (!strcmp(relName, RELCAT_RELNAME) || !strcmp(relName, ATTRCAT_RELNAME))
    {
        return E_NOTPERMITTED;
    }

    if (OpenRelTable::getRelId(relName) != E_RELNOTOPEN)
    {
        return E_RELOPEN;
    }

    return BlockAccess::deleteRelation(relName);
}

int Schema::createIndex(char relName[ATTR_SIZE], char attrName[ATTR_SIZE])
{
    if (!strcmp(relName, RELCAT_RELNAME) || !strcmp(relName, ATTRCAT_RELNAME))
    {
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);
    if (relId == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }

    return BPlusTree::bPlusCreate(relId, attrName);
}

int Schema::dropIndex(char *relName, char *attrName)
{
    if (!strcmp(relName, RELCAT_RELNAME) || !strcmp(relName, ATTRCAT_RELNAME))
    {
        return E_NOTPERMITTED;
    }
    int relId = OpenRelTable::getRelId(relName);
    if (relId == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }
    AttrCatEntry attrCatBuff;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuff);
    if (ret < 0)
    {
        return E_ATTRNOTEXIST;
    }
    int rootBlock = attrCatBuff.rootBlock;
    if (rootBlock == -1)
    {
        return E_NOINDEX;
    }
    BPlusTree::bPlusDestroy(rootBlock);
    attrCatBuff.rootBlock = -1;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatBuff);
    return SUCCESS;
}