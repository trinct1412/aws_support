from inspect import signature
from functools import wraps, reduce


def update(table_name):
    def get_update(func):
        @wraps(func)
        def execute(self, *args, key=None, callback_func=None, primary_key='id', **kwargs):
            assert table_name, "table not found"
            argument = list(signature(func).parameters.keys())[1:]
            try:
                kwargs = {**kwargs, **{k: args[i] for i, k in enumerate(argument) if not kwargs.get(k)}}
                pairing = {reduce(lambda result, key: result + f'{key}=:{key},', kwargs, '')[:-1]}
                raw_query = f"UPDATE {table_name} SET {pairing} WHERE {primary_key} = :key RETURNING id"
                result = self.connection.execute(raw_query, {**kwargs, f"key": key}).fetchone()
                if callback_func:
                    return callback_func(result)
                return result
            except(Exception,) as e:
                raise e

        return execute

    return get_update
