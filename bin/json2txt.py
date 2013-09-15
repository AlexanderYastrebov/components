#!/usr/bin/env python
import fileinput
import json


def main():

    for line in fileinput.input():
        if not line.strip():
            continue

        x = json.loads(line)
        print x['_srcId'] + '\t' + x['_dstId']


if __name__ == '__main__':
    main()
