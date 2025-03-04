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
    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);
    int src_nAttrs = srcRelCatEntry.numAttrs;
    char attr_names[src_nAttrs][ATTR_SIZE];
    int attr_types[src_nAttrs];

    for (int i = 0; i < src_nAttrs; i++)
    {
        AttrCatEntry attrCatBuffer;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatBuffer);
        strcpy(attr_names[i], attrCatBuffer.attrName);
        attr_types[i] = attrCatBuffer.attrType;
    }

    ret = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
    if (ret < 0)
    {
        return ret;
    }
    ret = OpenRelTable::openRel(targetRel);
    if (ret < 0)
    {
        Schema::deleteRel(targetRel);
        return ret;
    }
    int targetRelId = OpenRelTable::getRelId(targetRel);

    RelCacheTable::resetSearchIndex(srcRelId);
    Attribute record[src_nAttrs];

    /*
       The BlockAccess::search() function can either do a linearSearch or
       a B+ tree search. Hence, reset the search index of the relation in the
       relation cache using RelCacheTable::resetSearchIndex().
       Also, reset the search index in the attribute cache for the select
       condition attribute with name given by the argument `attr`. Use
       AttrCacheTable::resetSearchIndex().
       Both these calls are necessary to ensure that search begins from the
       first record.*/

    RelCacheTable::resetSearchIndex(srcRelId);
    AttrCacheTable::resetSearchIndex(srcRelId, attr);

    // read every record that satisfies the condition by repeatedly calling
    // BlockAccess::search() until there are no more records to be read

    while (1)
    {
        ret = BlockAccess::search(srcRelId, record, attr, attrVal, op);
        if (ret < 0)
        {
            break;
        }

        ret = BlockAccess::insert(targetRelId, record);
        if (ret < 0)
        {
            OpenRelTable::closeRel(targetRelId);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }
    OpenRelTable::closeRel(targetRelId);
    return SUCCESS;
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE])
{
    if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
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

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE])
{
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }
    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);
    int src_nAttrs = srcRelCatEntry.numAttrs;
    char attr_names[src_nAttrs][ATTR_SIZE];
    int attr_types[src_nAttrs];
    for (int i = 0; i < src_nAttrs; i++)
    {
        AttrCatEntry attrCatBuffer;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatBuffer);
        strcpy(attr_names[i], attrCatBuffer.attrName);
        attr_types[i] = attrCatBuffer.attrType;
    }

    int ret = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
    if (ret < 0)
    {
        return ret;
    }
    ret = OpenRelTable::openRel(targetRel);
    if (ret < 0)
    {
        Schema::deleteRel(targetRel);
        return ret;
    }
    int targetRelId = OpenRelTable::getRelId(targetRel);

    RelCacheTable::resetSearchIndex(srcRelId);
    Attribute record[src_nAttrs];
    while (1)
    {
        ret = BlockAccess::project(srcRelId, record);
        if (ret < 0)
        {
            break;
        }

        ret = BlockAccess::insert(targetRelId, record);
        if (ret < 0)
        {
            OpenRelTable::closeRel(targetRelId);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }
    OpenRelTable::closeRel(targetRelId);
    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE])
{
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }
    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);
    int src_nAttrs = srcRelCatEntry.numAttrs;
    int attr_offsets[tar_nAttrs];
    int attr_types[tar_nAttrs];
    for (int i = 0; i < tar_nAttrs; i++)
    {
        AttrCatEntry attrCatBuffer;
        int ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrCatBuffer);
        if (ret < 0)
        {
            return ret;
        }
        attr_offsets[i] = attrCatBuffer.offset;
        attr_types[i] = attrCatBuffer.attrType;
    }

    int ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
    if (ret < 0)
    {
        return ret;
    }
    ret = OpenRelTable::openRel(targetRel);
    if (ret < 0)
    {
        Schema::deleteRel(targetRel);
        return ret;
    }
    int targetRelId = OpenRelTable::getRelId(targetRel);

    RelCacheTable::resetSearchIndex(srcRelId);
    Attribute record[src_nAttrs];
    while (1)
    {
        Attribute projRecord[tar_nAttrs];
        ret = BlockAccess::project(srcRelId, record);
        if (ret < 0)
        {
            break;
        }
        for (int i = 0; i < tar_nAttrs; i++)
        {
            projRecord[i] = record[attr_offsets[i]];
        }

        ret = BlockAccess::insert(targetRelId, projRecord);
        if (ret < 0)
        {
            OpenRelTable::closeRel(targetRelId);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }
    OpenRelTable::closeRel(targetRelId);
    return SUCCESS;
}

int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE])
{
    int relIdsrc1 = OpenRelTable::getRelId(srcRelation1);
    int relIdsrc2 = OpenRelTable::getRelId(srcRelation2);

    if (relIdsrc1 == E_RELNOTOPEN || relIdsrc2 == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry1, attrCatEntry2;

    int ret1 = AttrCacheTable::getAttrCatEntry(relIdsrc1, attribute1, &attrCatEntry1);
    int ret2 = AttrCacheTable::getAttrCatEntry(relIdsrc2, attribute2, &attrCatEntry2);

    if (ret1 != SUCCESS || ret2 != SUCCESS)
    {
        return E_ATTRNOTEXIST;
    }
    if (attrCatEntry1.attrType != attrCatEntry2.attrType)
    {
        return E_ATTRTYPEMISMATCH;
    }

    // iterate through all the attributes in both the source relations and check if
    // there are any other pair of attributes other than join attributes
    // (i.e. attribute1 and attribute2) with duplicate names in srcRelation1 and
    // srcRelation2 (use AttrCacheTable::getAttrCatEntry())
    // If yes, return E_DUPLICATEATTR

    RelCatEntry relCatEntry1, relCatEntry2;
    RelCacheTable::getRelCatEntry(relIdsrc1, &relCatEntry1);
    RelCacheTable::getRelCatEntry(relIdsrc2, &relCatEntry2);

    int numOfAttributes1 = relCatEntry1.numAttrs;
    int numOfAttributes2 = relCatEntry2.numAttrs;

    for (int attrindex1 = 0; attrindex1 < numOfAttributes1; attrindex1++)
    {
        AttrCatEntry attrCatEntryTemp1;
        AttrCacheTable::getAttrCatEntry(relIdsrc1, attrindex1, &attrCatEntryTemp1);

        if (strcmp(attrCatEntryTemp1.attrName, attribute1) == 0)
            continue;

        for (int attrindex2 = 0; attrindex2 < numOfAttributes2; attrindex2++)
        {
            AttrCatEntry attrCatEntryTemp2;
            AttrCacheTable::getAttrCatEntry(relIdsrc2, attrindex2, &attrCatEntryTemp2);

            if (strcmp(attrCatEntryTemp2.attrName, attribute2) == 0)
                continue;

            if (strcmp(attrCatEntryTemp1.attrName, attrCatEntryTemp2.attrName) == 0)
                return E_DUPLICATEATTR;
        }
    }

    if (attrCatEntry2.rootBlock == -1)
    {
        if (BPlusTree::bPlusCreate(relIdsrc2, attribute2) == E_DISKFULL)
        {
            return E_DISKFULL;
        }
    }
    int numOfAttributesInTarget = numOfAttributes1 + numOfAttributes2 - 1;
    char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
    int targetRelAttrTypes[numOfAttributesInTarget];

    for (int i = 0; i < numOfAttributes1; i++)
    {
        AttrCatEntry attrCatBuffer;
        AttrCacheTable::getAttrCatEntry(relIdsrc1, i, &attrCatBuffer);
        strcpy(targetRelAttrNames[i], attrCatBuffer.attrName);
        targetRelAttrTypes[i] = attrCatBuffer.attrType;
    }
    for (int i = 0; i < numOfAttributes2; i++)
    {
        AttrCatEntry attrCatBuffer;
        AttrCacheTable::getAttrCatEntry(relIdsrc2, i, &attrCatBuffer);
        bool inserted = false;
        if (strcmp(attrCatBuffer.attrName, attribute2) == 0)
        {
            inserted = true;
            continue;
        }
        if (inserted != 0)
        {
            strcpy(targetRelAttrNames[numOfAttributes1 + i - 1], attrCatBuffer.attrName);
            targetRelAttrTypes[numOfAttributes1 + i - 1] = attrCatBuffer.attrType;
        }
        else
        {
            strcpy(targetRelAttrNames[numOfAttributes1 + i], attrCatBuffer.attrName);
            targetRelAttrTypes[numOfAttributes1 + i] = attrCatBuffer.attrType;
        }
    }
    int ret = Schema::createRel(targetRelation, numOfAttributesInTarget, targetRelAttrNames, targetRelAttrTypes);
    if (ret < 0)
    {
        return ret;
    }
    ret = OpenRelTable::openRel(targetRelation);
    if (ret < 0)
    {
        Schema::deleteRel(targetRelation);
        return ret;
    }

    Attribute record1[numOfAttributes1];
    Attribute record2[numOfAttributes2];
    Attribute targetRecord[numOfAttributesInTarget];

    RelCacheTable::resetSearchIndex(relIdsrc1);

    while (BlockAccess::project(relIdsrc1, record1) == SUCCESS)
    {
        AttrCacheTable::resetSearchIndex(relIdsrc2, attribute2);
        RelCacheTable::resetSearchIndex(relIdsrc2);

        while (BlockAccess::search(
                   relIdsrc2, record2, attribute2, record1[attrCatEntry1.offset], EQ) == SUCCESS)
        {
            for (int i = 0; i < numOfAttributes1; i++)
            {
                targetRecord[i] = record1[i];
            }
            for (int i = 0, flag = 0; i < numOfAttributes2; i++)
            {
                if (i == attrCatEntry2.offset)
                {
                    flag = 1;
                    continue;
                }
                targetRecord[i + numOfAttributes1 - flag] = record2[i];
            }
            if (BlockAccess::insert(OpenRelTable::getRelId(targetRelation), targetRecord) == E_DISKFULL)
            {
                OpenRelTable::closeRel(OpenRelTable::getRelId(targetRelation));
                Schema::deleteRel(targetRelation);
                return E_DISKFULL;
            }
        }
    }
    OpenRelTable::closeRel(OpenRelTable::getRelId(targetRelation));
    return SUCCESS;
}