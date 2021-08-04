#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char *argv[])
{
    const char * filename = "samples.bin";
    FILE * sample_file = fopen(filename, "w");
    fprintf(sample_file, "%d\n", 12345);
    int    a = 0x12345678;
    double b = 3.1415926535;
    float
        c = 0.1,
        d = 0.2,
        e = 0.3;
    unsigned int buffer_size = sizeof(int) + 20*sizeof(int) + sizeof(double) + sizeof(float) * 3;
    unsigned char * buffer = (unsigned char *)malloc(buffer_size);
    unsigned int offset = 0;

    int * int_ptr = (int *)(buffer);
    *int_ptr = 20;
    offset += sizeof(int);

    int arr[20] =
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};

    memcpy(buffer + offset, arr, 20*sizeof(int));
    offset += 20*sizeof(int);

    int_ptr = (int *)(buffer + offset);
    *int_ptr = a;
    offset += sizeof(int);

    double * b_ptr = (double *)(buffer + offset);
    *b_ptr = b;
    offset += sizeof(double);

    typedef struct {
        float x;
        float y;
        float z;
    } floats;

    floats * floats_ptr = (floats *)(buffer + offset);
    *floats_ptr = (floats){.x=c, .y=d, .z=e};
    offset += sizeof(floats);

    fwrite(buffer, 1, offset, sample_file);
    fclose(sample_file);
    return 0;
}
