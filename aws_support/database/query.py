from sqlalchemy import text
from inspect import signature
from functools import wraps


def queryset(method_name):
    def get_queryset(func):
        @wraps(func)
        def execute(self, *args, callback_func=None, is_local=False, **kwargs):
            query = None
            raw_query = None
            argument = list(signature(func).parameters.keys())[1:]
            try:
                kwargs = {**kwargs, **{k: args[i] for i, k in enumerate(argument) if not kwargs.get(k)}}
                argument = {k: kwargs.get(k, None) for k in argument}
                raw_query = text(func(self, **argument))
                query = self.connection.execute(raw_query, kwargs)
                method = getattr(query, method_name)
                result = method()
                if callback_func:
                    return callback_func(result)
                return result
            except AttributeError:
                raise NotImplementedError(
                    "Class `{}` does not implement `{}`".format(query.__class__.__name__, method_name))
            except(Exception,) as e:
                raise e
            finally:
                if is_local:
                    params = {param: kwargs[param] for param in raw_query.compile().params}
                    print(f"--------------------------{func.__name__}-----------------------------------")
                    print(raw_query.bindparams(**params).compile(compile_kwargs={"literal_binds": True}))
                    print("-----------------------------------------------------------------------------")

        assert method_name, "required method name"
        return execute

    return get_queryset


def filter_query(func):
    @wraps(func)
    def execute(self, *args, callback_func=None, **kwargs):
        argument = list(signature(func).parameters.keys())[1:]
        try:
            kwargs = {**kwargs, **{k: args[i] for i, k in enumerate(argument) if not kwargs.get(k)}}
            argument = {k: kwargs.get(k, None) for k in argument}
            filter_ = kwargs.get('filter_', {})
            raw_query = "( "
            raw_query += func(self, **argument)
            if "WHERE" in raw_query:
                raise Exception("support query not using WHERE in clause")
            if filter_:
                raw_query += " WHERE "
            for i, (k, v) in enumerate(filter_.items()):
                raw_query += f"{k} = '{v}' "
                if i < len(filter_) - 1:
                    raw_query += "AND "
            else:
                raw_query += " )"
            result = text(raw_query)
            if callback_func:
                return callback_func(result)
            return result
        except(Exception,) as e:
            raise e

    return execute
