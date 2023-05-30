from inspect import signature
from functools import wraps, reduce


def insert(table_name):
    def get_insert(func):
        @wraps(func)
        def execute(self, *args, callback_func=None, **kwargs):
            assert table_name, "table not found"
            argument = list(signature(func).parameters.keys())[1:]
            try:
                kwargs = {**kwargs, **{k: args[i] for i, k in enumerate(argument) if not kwargs.get(k)}}

                key = reduce(lambda result_, key_: result_ + f'{key_},', kwargs, '')[:-1]
                value = reduce(lambda result_, key_: result_ + f':{key_},', kwargs, '')[:-1]

                pairing = f"({key})VALUES({value}RETURNING id)"
                raw_query = f"INSERT INTO {table_name}{pairing} RETURNING id"
                result = self.connection.execute(raw_query).fetchone()
                if callback_func:
                    return callback_func(result)
                return result
            except(Exception,) as e:
                raise e

        return execute

    return get_insert
