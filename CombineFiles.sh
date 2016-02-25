#!/bin/bash

while getopts ":d:" opt; do
    case $opt in 
        d)
            # echo "-d was triggered, Parameter: $OPTARG" >&2
            cd $OPTARG
            pwd
            ;;
        i)
            cd $OPTARG
            pwd
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
        :)
            echo "Option: -$OPTARG requires an argument." >&2
            exit 1
            ;;
    esac
done


tail -n +2 -q file* > out.tmp
head -n +1 -q file1.tmp > a.tmp
cat a.tmp out.tmp > out.csv
# rm *.tmp

