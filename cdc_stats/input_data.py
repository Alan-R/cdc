#!/usr/bin/env python
# coding=utf-8
#
# vim: smartindent tabstop=4 shiftwidth=4 expandtab number colorcolumn=100
#
# Author: Alan Robertson <alanr@unix.sh>
#
# This software is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# The Assimilation software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this software.
# If not, see http://www.gnu.org/licenses/
"""

"""
from typing import List, Dict, Tuple, Callable, Union
from collections import namedtuple
import csv
import datetime
import json
import requests

FieldTypes = Union[int, str, datetime.date]

class FieldEncoder(json.JSONEncoder):
    def default(self, thing):
        if isinstance(thing, datetime.date):
            return f"{thing.year:04d}-{thing.month:02d}-{thing.day:02d}"
        return thing


class UrlCSV:
    """
    A class to create a typeful CSV object from a single URL
    """

    def __init__(self, name: str, url: str):
        response: requests.Response = requests.get(url=url)
        response.raise_for_status()
        csv_string: str = response.content.decode('utf-8')
        dialect = csv.Sniffer().sniff(csv_string[:1024])
        reader = csv.reader(csv_string.splitlines(), dialect=dialect)
        headings = reader.__next__()
        fields = self._clean_headings(headings)
        self.csv_type: Callable = namedtuple(name, fields)
        self.data: List[namedtuple] = []
        for row in reader:
            self.data.append(self.csv_type(*row))
        self.field_names = fields

        self.field_types: List[Tuple[Callable, type]] = self.determine_types()
        self.typed_data = self._make_typed_tuples()
        self.typed_dict: List[Dict[str, FieldTypes]] = self._make_typed_dict()

    @staticmethod
    def _clean_headings(headings: List[str]) -> List[str]:
        fields: List[str] = []
        for name in headings:
            field = name.replace('(', '_')
            field = field.replace(')', '_')
            field = field.replace(',', '_')
            field = field.replace('-', '_')
            field = field.replace(' ', '_')
            field = field.replace('__', '_')
            while field.endswith('_'):
                field = field[:-1]
            fields.append(field)
        return fields

    def determine_types(self) -> Dict[str, Tuple[Callable, type]]:
        field_types: Dict[str, Tuple[Callable, type]] = {}
        detect_funcs = [
            (int, int),
            (datetime.date.fromisoformat, datetime.date),
            (str, str)
        ]
        for row in self.data:
            print(f"LOOKING AT {row}")
            for field in self.field_names:
                value = getattr(row, field)
                if value == '':
                    continue
                field_type = None
                field_func = None
                for (func, cls) in detect_funcs:
                    try:
                        func(value)
                        field_type = cls
                        field_func = func
                        break
                    except (ValueError, TypeError):
                        pass
                if field_type is None:
                    raise RuntimeError("Somehow it's not even a string")
                prev_value = field_types.get(field, None)
                if prev_value is not None:
                    if prev_value != (field_func, field_type):
                        raise RuntimeError(f"Mismatching types: {prev_value[0]} vs {field_type}")
                else:
                    field_types[field] = (field_func, field_type)

        for field in self.field_names:
            if field not in field_types:
                field_types[field] = (str, str)
        return field_types

    def _make_typed_tuples(self):
        result = []
        for row in self.data:
            typed_row: List[FieldTypes] = []
            for field in self.field_names:
                func = self.field_types[field][0]
                value = getattr(row, field)
                if value != '':
                    value = func(getattr(row, field))
                else:
                    value = None
                typed_row.append(value)
            result.append(self.csv_type(*typed_row))
        return result

    def _make_typed_dict(self) -> List[Dict[str, FieldTypes]]:
        result: List[Dict[str, FieldTypes]] = []
        for row in self.typed_data:
            row_dict: Dict[str, FieldTypes] = {}
            for field in self.field_names:
                row_dict[field] = getattr(row, field)
            result.append(row_dict)
        return result





def testme():
    our_csv = UrlCSV("cdc", "https://data.cdc.gov/api/views/muzy-jte6/rows.csv")
    # print(csv.data)
    print(our_csv.field_types)
    for row in our_csv.typed_dict:
        print(json.dumps(row, cls=FieldEncoder, indent=4))


if __name__ == '__main__':
    testme()
