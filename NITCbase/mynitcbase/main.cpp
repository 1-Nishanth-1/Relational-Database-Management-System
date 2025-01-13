#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <cstring>
#include <iostream>
using namespace std;

void stage1()
{
    unsigned char buffer[BLOCK_SIZE];
    char message[] = "hello";
    memcpy(buffer, message, 6);
    Disk::writeBlock(buffer, 7000);

    unsigned char buffer2[BLOCK_SIZE];
    char message2[6];
    Disk::readBlock(buffer2, 0);
    memcpy(message2, buffer2, 6);
    cout << message2 << endl;
}

void stage1_mod()
{
    unsigned char buffer2[BLOCK_SIZE];
    Disk::readBlock(buffer2, 0);

    for (int i = 0; i < BLOCK_SIZE; i++)
    {
        printf("%u ", buffer2[i]);
    }
}

void printSchema_Stage2()
{
    RecBuffer relCatBuffer(RELCAT_BLOCK);
    RecBuffer attrCatBuffer(ATTRCAT_BLOCK);

    HeadInfo relCatHeader;
    HeadInfo attrCatHeader;

    relCatBuffer.getHeader(&relCatHeader);
    attrCatBuffer.getHeader(&attrCatHeader);

    for (int i = 0; i < relCatHeader.numEntries; i++)
    {

        Attribute relCatRecord[RELCAT_NO_ATTRS];

        relCatBuffer.getRecord(relCatRecord, i);

        printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

        for (int j = 0; j < attrCatHeader.numEntries; j++)
        {

            Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

            attrCatBuffer.getRecord(attrCatRecord, j);

            if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relCatRecord[RELCAT_REL_NAME_INDEX].sVal) == 0)
            {

                const char *attrtype = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
                printf("\t%s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrtype);

                // Question 1

                if (j + 1 == attrCatHeader.numSlots)
                {
                    j = -1;
                    attrCatBuffer = RecBuffer(attrCatHeader.rblock);
                    attrCatBuffer.getHeader(&attrCatHeader);
                }
            }
        }
        printf("\n");
    }
}

void modifyAttribute_Stage2Q2(const char *relation, const char *oldAttribute, const char *newAttribute)
{
    RecBuffer relCatBuffer(RELCAT_BLOCK);
    RecBuffer attrCatBuffer(ATTRCAT_BLOCK);

    HeadInfo relCatHeader;
    HeadInfo attrCatHeader;

    relCatBuffer.getHeader(&relCatHeader);
    attrCatBuffer.getHeader(&attrCatHeader);

    for (int i = 0; i < relCatHeader.numEntries; i++)
    {

        Attribute relCatRecord[RELCAT_NO_ATTRS];

        relCatBuffer.getRecord(relCatRecord, i);
        if (strcmp(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relation) == 0)
        {

            for (int j = 0; j < attrCatHeader.numEntries; j++)
            {
                Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

                attrCatBuffer.getRecord(attrCatRecord, j);

                Attribute newattrCatRecord[ATTRCAT_NO_ATTRS];

                for (int k = 0; k < ATTRCAT_NO_ATTRS; k++)
                {
                    newattrCatRecord[k] = attrCatRecord[k];
                    if (k == ATTRCAT_ATTR_NAME_INDEX)
                    {
                        strcpy(newattrCatRecord[k].sVal, newAttribute);
                    }
                }

                if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relCatRecord[RELCAT_REL_NAME_INDEX].sVal) == 0 && strcmp(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldAttribute) == 0)
                {

                    attrCatBuffer.setRecord(newattrCatRecord, j);
                    cout << "Updated Successfull\n";
                    return;
                }
                if (j + 1 == attrCatHeader.numSlots)
                {
                    if (attrCatHeader.rblock != -1)
                    {
                        j = -1;
                        attrCatBuffer = RecBuffer(attrCatHeader.rblock);
                        attrCatBuffer.getHeader(&attrCatHeader);
                    }
                    else
                    {
                        cout << "Attribute Not Found\n";
                        return;
                    }
                }
            }
        }
    }
    cout << "Attribute Not Found\n";
    return;
}

int main(int argc, char *argv[])
{
    Disk disk_run;
    StaticBuffer buffer;
    OpenRelTable cache;

    for (int i = 0; i <= 1; i++)
    {
        RelCatEntry relCatBuffer;
        RelCacheTable::getRelCatEntry(i, &relCatBuffer);
        printf("Relation: %s\n", relCatBuffer.relName);
        for (int j = 0; j < relCatBuffer.numAttrs; j++)
        {
            AttrCatEntry attrCatBuffer;
            AttrCacheTable::getAttrCatEntry(i, j, &attrCatBuffer);
            const char *attrtype = attrCatBuffer.attrType == NUMBER ? "NUM" : "STR";
            printf("\t%s: %s\n", attrCatBuffer.attrName, attrtype);
        }
        printf("\n");
    }

    // modifyAttribute_Stage2Q2("Students", "Batch", "Class");
    // printSchema_Stage2();
    // stage1_mod();
    return 0;
}
