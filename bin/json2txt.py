#!/usr/bin/env python
import fileinput
import json
import codecs
import sys

UTF8Writer = codecs.getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)

def main():

    for line in fileinput.input():
        if not line.strip():
            continue

        x = json.loads(line)
        print x['_srcId'] + '\t' + x['_dstId']


if __name__ == '__main__':
    main()

