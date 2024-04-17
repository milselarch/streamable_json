import copy
import json

from enum import StrEnum
from typing import Any, Tuple, List, Optional
from dataclasses import dataclass
from result import Result, Ok, Err


class ContextTypes(StrEnum):
    OBJECT = '{'
    ARRAY = '['


@dataclass
class Context(object):
    type: ContextTypes
    start_index: int
    end_index: int

    items_inserted: int = 0


class JsonStreamWriter(object):
    def __init__(self, filepath: str):
        self._file_obj = open(filepath, 'w+')
        self._context_stack: List[Context] = []
        self._add_object_context()

        self.context_manager_held = False
        self.closed = False

    def __enter__(self):
        assert not self.context_manager_held
        self.context_manager_held = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.context_manager_held = False
        self.close()

    def add_array_context(
        self, key: Optional[str] = None
    ) -> Result[None, Exception]:
        get_current_context_result = self.get_current_context()
        if get_current_context_result.is_err():
            return get_current_context_result

        current_context = get_current_context_result.unwrap()
        if (key is None) and (current_context.type == ContextTypes.OBJECT):
            return Err(ValueError('Context key cannot be None'))

        if current_context.items_inserted > 0:
            self._file_obj.write(', ')

        end_idx = start_idx = self._file_obj.tell() + 1
        if current_context.type == ContextTypes.OBJECT:
            encoded_key = json.dumps(key)
            self._file_obj.write(encoded_key + ': ')

        self._file_obj.write(str(ContextTypes.ARRAY))
        self._context_stack.append(Context(
            type=ContextTypes.ARRAY,
            start_index=start_idx, end_index=end_idx
        ))

    def add_object_context(self, key: Optional[str] = None):
        assert len(self._context_stack) == 0
        return self._add_object_context(key)

    def _add_object_context(
        self, key: Optional[str] = None
    ) -> Result[None, Exception]:
        get_current_context_result = self.get_current_context()
        if get_current_context_result.is_err():
            return get_current_context_result

        current_context = get_current_context_result.unwrap()
        if (key is None) and (current_context.type == ContextTypes.OBJECT):
            return Err(ValueError('Context key cannot be None'))

        if current_context.items_inserted > 0:
            self._file_obj.write(', ')

        end_idx = start_idx = self._file_obj.tell() + 1
        if current_context.type == ContextTypes.OBJECT:
            encoded_key = json.dumps(key)
            self._file_obj.write(encoded_key + ': ')

        current_context.items_inserted += 1
        self._file_obj.write(str(ContextTypes.OBJECT))
        self._context_stack.append(Context(
            type=ContextTypes.OBJECT,
            start_index=start_idx, end_index=end_idx
        ))

    def get_current_context(self) -> Result[Context, Exception]:
        if len(self._context_stack) == 0:
            return Err(KeyError('No current context'))

        return Ok(copy.deepcopy(self._context_stack[-1]))

    def add_array_item(self, item: Any) -> Result[Tuple[int, int], Exception]:
        """
        Add a value to the current array context.
        :param item: array item
        :return:
        the start and end index within the file of the item inserted
        """
        get_current_context_result = self.get_current_context()
        if get_current_context_result.is_err():
            return get_current_context_result

        current_context = get_current_context_result.unwrap()
        if current_context.items_inserted > 0:
            self._file_obj.write(', ')

        start_idx = self._file_obj.tell() + 1
        chars_written = self._file_obj.write(json.dumps(item))
        end_idx = self._file_obj.tell() + 1

        current_context.items_inserted += 1
        current_context.end_index += chars_written
        return Ok((start_idx, end_idx))

    def add_object_item(
        self, key: str, value: Any
    ) -> Result[Tuple[int, int], Exception]:
        """
        Add a key-value pair to the current object context.
        :param key: json item key
        :param value: json item value
        :return:
        the start and end index within the file of the key-value pair
        """
        get_current_context_result = self.get_current_context()
        if get_current_context_result.is_err():
            return get_current_context_result

        current_context = get_current_context_result.unwrap()
        if current_context.items_inserted > 0:
            self._file_obj.write(', ')

        start_idx = self._file_obj.tell() + 1
        entry = f'{json.dumps(key)}: {json.dumps(value)}'
        chars_written = self._file_obj.write(entry)
        end_idx = self._file_obj.tell() + 1

        current_context.items_inserted += 1
        current_context.end_index += chars_written
        return Ok((start_idx, end_idx))

    def close_current_context(self) -> bool:
        if len(self._context_stack) == 0:
            return False

        context = self._context_stack.pop()

        if context.type == ContextTypes.ARRAY:
            self._file_obj.write(']')
        elif context.type == ContextTypes.OBJECT:
            self._file_obj.write('}')

        return True

    def close(self):
        while len(self._context_stack) > 0:
            self.close_current_context()

        assert len(self._context_stack) == 0
        self._file_obj.close()
        self.closed = True