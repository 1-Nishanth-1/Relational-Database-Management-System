#include "Schema.h"
#include "iostream"
#include <cmath>
#include <cstring>

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
    return SUCCESS;
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
