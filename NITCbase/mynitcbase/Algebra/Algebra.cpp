#include "Algebra.h"
#include <cstdlib>
#include <cstring>
#include <iostream>

bool isNumber(char *str)
{
    int len;
    float ignore;
    /*
   sscanf returns the number of elements read, so if there is no float matching
   the first %f, ret will be 0, else it'll be 1

   %n gets the number of characters read. this scanf sequence will read the
   first float ignoring all the whitespace before and after. and the number of
   characters read that far will be stored in len. if len == strlen(str), then
   the string only contains a float with/without whitespace. else, there's other
   characters.
 */
    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE])
{
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
    if (ret != SUCCESS)
    {
        return E_ATTRNOTEXIST;
    }

    int type = attrCatEntry.attrType;
    Attribute attrVal;
    if (type == NUMBER)
    {
        if (isNumber(strVal))
        {
            attrVal.nVal = atof(strVal);
        }
        else
        {
            return E_ATTRTYPEMISMATCH;
        }
    }
    else if (type == STRING)
    {
        strcpy(attrVal.sVal, strVal);
    }

    RelCacheTable::resetSearchIndex(srcRelId);

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    printf("|");
    for (int i = 0; i < relCatEntry.numAttrs; ++i)
    {
        AttrCatEntry attrCatEntry;

        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

        printf(" %s |", attrCatEntry.attrName);
    }
    printf("\n");

    while (true)
    {

        RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);
        if (searchRes.block != 1 && searchRes.slot != -1)
        {
            AttrCatEntry attrCatBuffer;

            RecBuffer blockBuffer(searchRes.block);
            HeadInfo header;
            blockBuffer.getHeader(&header);
            Attribute record[header.numAttrs];
            blockBuffer.getRecord(record, searchRes.slot);
            printf("|");
            for (int i = 0; i < header.numAttrs; i++)
            {
                AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatBuffer);
                if (attrCatBuffer.attrType == NUMBER)
                {
                    printf(" %d |", (int)record[i].nVal);
                }
                else
                {
                    printf(" %s |", record[i].sVal);
                }
            }
            printf("\n");
        }
        else
        {
            break;
        }
    }
    return SUCCESS;
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE])
{
    if (relName == RELCAT_RELNAME || relName == ATTRCAT_RELNAME)
    {
        E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);

    if (relId == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }

    RelCatEntry relCatBuffer;

    RelCacheTable::getRelCatEntry(relId, &relCatBuffer);

    if (relCatBuffer.numAttrs != nAttrs)
    {
        return E_NATTRMISMATCH;
    }

    union Attribute recordValues[nAttrs];

    AttrCatEntry attrCatBuffer;

    for (int i = 0; i < nAttrs; i++)
    {
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatBuffer);
        int type = attrCatBuffer.attrType;
        if (type == NUMBER)
        {
            if (isNumber(record[i]))
            {
                recordValues[i].nVal = atof(record[i]);
            }
            else
            {
                E_ATTRTYPEMISMATCH;
            }
        }
        else if (type == STRING)
        {
            strcpy(recordValues[i].sVal, record[i]);
        }
    }
    return BlockAccess::insert(relId, recordValues);
}