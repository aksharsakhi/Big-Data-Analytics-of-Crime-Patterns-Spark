#!/usr/bin/env python3
"""
Unit Test Script for Crime Record CSV Parser.
Simulates Scala parser line tokenization and field extraction.
"""

import unittest

def parse_line(line):
    tokens = [t.strip().strip('"') for t in line.split(",")]
    if len(tokens) < 17:
        tokens = tokens + [""] * (17 - len(tokens))
    return {
        "id": tokens[0],
        "case_number": tokens[1],
        "date": tokens[2],
        "primary_type": tokens[5],
        "arrest": tokens[8].lower() == "true",
        "district": tokens[11]
    }

class TestCrimeParser(unittest.TestCase):
    def test_standard_line(self):
        line = '10001,HY189342,03/18/2015 11:00:00 PM,043XX S WOOD ST,0486,BATTERY,DOMESTIC BATTERY,RESIDENCE,true,true,0924,009,12,61,2015,41.815,-87.671'
        record = parse_line(line)
        self.assertEqual(record["id"], "10001")
        self.assertEqual(record["primary_type"], "BATTERY")
        self.assertTrue(record["arrest"])
        self.assertEqual(record["district"], "009")

    def test_boolean_conversion(self):
        line_false = '10002,HY189343,03/18/2015 11:30:00 PM,022XX W 21ST ST,0820,THEFT,$500 AND UNDER,STREET,false,false,1034,010,25,31,2015,41.853,-87.682'
        record = parse_line(line_false)
        self.assertFalse(record["arrest"])

if __name__ == "__main__":
    unittest.main()
