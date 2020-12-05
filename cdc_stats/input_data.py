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
This code inputs data from a CSV URL into a few suitable Python formats
"""
from typing import List, Dict, Tuple, Callable, Union, Optional
from collections import namedtuple
import csv
import datetime
import json
import re
import pandas as pd
import requests

FieldTypes = Union[int, str, datetime.date]


class FieldEncoder(json.JSONEncoder):
    def default(self, thing):
        if isinstance(thing, datetime.date):
            return f"{thing.year:04d}-{thing.month:02d}-{thing.day:02d}"
        return thing


def date_to_datetime_date(date_str: str) -> datetime.date:
    if '/' in date_str:
        month, date, year = date_str.split('/')
        date_str = f"{year}-{month}-{date}"
    return datetime.date.fromisoformat(date_str)


class UrlCSV:
    """
    A class to create a typeful and flexible CSV object from a single URL
    """

    def __init__(self, name: str, url: str):
        """
        Initializer for UrlCSV class

        :param name: str: name of this type (as a namedtuple)
        :param url: str: URL of where to find the CSV data
        """
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

        self.field_types: Dict[str, Tuple[Callable, type]] = self.determine_types()
        self.typed_data = self._make_typed_tuples()
        self.typed_dict: List[Dict[str, FieldTypes]] = self._make_typed_dict()

    @staticmethod
    def _clean_headings(headings: List[str]) -> List[str]:
        """
        Clean up headings - not perfect, but it works for now...
        :param headings: List[str]: List of heading names from the CSV
        :return: List[str]: Cleaned up names suitable for being Python names
        """
        fields: List[str] = []
        # We're deleting these because they make life harder, and differ between years...
        del_regex1 = re.compile(r'([A-Z][0-9][0-9]-[A-Z][0-9][0-9])')
        del_regex2 = re.compile(r'([A-Z][0-9][0-9][0-9]?)')

        for name in headings:
            field = del_regex1.sub("", name)
            field = del_regex2.sub("", field)
            field = field.replace('(', '_')
            field = field.replace(')', '_')
            field = field.replace(',', '_')
            field = field.replace('-', '_')
            field = field.replace(' ', '_')
            field = field.replace('___', '_')
            field = field.replace('__', '_')
            while field.endswith('_'):
                field = field[:-1]
            fields.append(field)
        print("FIELDS:", fields)
        return fields

    def determine_types(self) -> Dict[str, Tuple[Callable, type]]:
        """
        Figure out what types the fields are from looking at the data
        :return: dict: Dictionary describing the callables to do conversions and field types
        """
        field_types: Dict[str, Tuple[Callable, type]] = {}
        detect_funcs = [
            (int, int),
            (date_to_datetime_date, datetime.date),
            (str, str)
        ]
        for row in self.data:
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
        """
        Make the typed data tuples out of the original untyped versions
        :return: List[Tuple]: A list of typed tuples
        """
        result = []
        for row in self.data:
            typed_row: List[FieldTypes] = []
            for field in self.field_names:
                func = self.field_types[field][0]
                value = getattr(row, field)
                if value != '':
                    value = func(getattr(row, field))
                elif self.field_types[field][1] is int:
                    value = 0
                else:
                    value = None
                typed_row.append(value)
            result.append(self.csv_type(*typed_row))
        return result

    def _make_typed_dict(self) -> List[Dict[str, FieldTypes]]:
        """
        Make the typed dict version of our typed tuples
        :return: List[Dict[str, FieldTypes]]: The list of typed dicts
        """
        result: List[Dict[str, FieldTypes]] = []
        for row in self.typed_data:
            row_dict: Dict[str, FieldTypes] = {}
            for field in self.field_names:
                row_dict[field] = getattr(row, field)
            result.append(row_dict)
        return result


def union_fields(*args: UrlCSV) -> List[str]:
    """
    Create an ordered list of fields that's the union of the fields in 'args'
    The assumption is that they are generally somewhat similar, and that we want to
    make sure we have the fields in about the same order, but all fields from
    any of the UrlCSVs.

    :return: List[str]
    """
    result: List[str] = []
    next_field: Optional[str] = None
    longest_field = 0
    longest_fields: List[str] = []
    for csv_arg in args:
        if len(csv_arg.field_names) > longest_field:
            longest_fields = [item for item in csv_arg.field_names]
            longest_field = len(longest_fields)
    for csv_arg in args:
        if csv_arg.field_names == longest_fields:
            continue
        offset = 0
        for index, field in enumerate(csv_arg.field_names):
            if field not in longest_fields:
                longest_fields.insert(index + offset, field)
                offset += 1
    return longest_fields


def merge_typed_dicts(*args: UrlCSV) -> List[Dict[str, FieldTypes]]:
    """
    Merge lists of typed dictionaries from multiple UrlCSVs
    This allows us to combine years with slightly different data
    The set of fields in every row is the same.
    Default values (None or 0) are provided for missing data

    :param args:UrlCSV: list of typed dicts to combine
    :return:merged (appended) typed dicts
    """
    csv_result: List[Dict[str, FieldTypes]] = []
    field_list = union_fields(*args)
    for csv_arg in args:
        for row_dict in csv_arg.typed_dict:
            new_row_dict: Dict[str, FieldTypes] = {}
            for field in field_list:
                default = None if 'flag' in field else 0
                new_row_dict[field] = row_dict.get(field, default)
            csv_result.append(new_row_dict)
    return csv_result


def typed_dict_to_typed_csv(td: List[Dict[str, FieldTypes]]) -> List[List[FieldTypes]]:
    """

    :param td:
    :return:
    """
    common_keys: List[str] = list(td[0].keys())
    result: List[List[FieldTypes]] = [common_keys]
    for row in td:
        row_data: List[FieldTypes] = []
        assert len(row) == len(common_keys)
        for field in common_keys:
            row_data.append(row[field])
        result.append(row_data)

    return result


def pivot_typed_dict(td: List[Dict[str, FieldTypes]]) -> Dict[str, List[FieldTypes]]:
    """
    Create a Pivot-typed-dict version of a TypedDict - this is for Pandas DataFrames
    :param td:
    :return:
    """
    result: Dict[str, List[FieldTypes]] = {key: [] for key in td[0].keys()}
    for row in td:
        for field in row.keys():
            result[field].append(row[field])
    return result


if __name__ == '__main__':
    def testme():
        our_csv = UrlCSV("cdc", "https://data.cdc.gov/api/views/muzy-jte6/rows.csv")
        csv_fields = set(our_csv.field_names)
        our_csv2 = UrlCSV("cdc2", "https://data.cdc.gov/api/views/3yf8-kanr/rows.csv")
        csv2_fields = set(our_csv2.field_names)
        # print(csv.data)
        print(our_csv.field_types)
        print(our_csv2.field_types)
        print(csv_fields - csv2_fields)
        print(csv2_fields - csv_fields)
        merged_dicts = merge_typed_dicts(our_csv2, our_csv)
        j = 0
        for row in merged_dicts:
            j += 1
            print(json.dumps(row, cls=FieldEncoder, indent=4))
            if j > 20:
                break
        typed_csv_thing = typed_dict_to_typed_csv(merged_dicts)
        j = 0
        for row in typed_csv_thing[:3]:
            print(row)
        for row in typed_csv_thing[-3:]:
            print(row)
        pivot_dict = pivot_typed_dict(merged_dicts)
        for key in pivot_dict.keys():
            print(f"{key}: {pivot_dict[key][:10]}...")
        data_frame = pd.DataFrame(pivot_dict)
        print(data_frame[['MMWR_Year', 'MMWR_Week', 'All_Cause',
                          'COVID_19_Multiple_Cause_of_Death',
                          'COVID_19_Underlying_Cause_of_Death']].tail())

    testme()
