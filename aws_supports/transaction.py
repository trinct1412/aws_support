import functools


def transaction_db(is_commit=False):
    def decorator(func):
        @functools.wraps(func)
        def execute(self, *args, **kwargs):
            try:
                result = func(self, *args, **kwargs)
                if is_commit:
                    self.connection.commit()
                return result
            except (Exception,) as e:
                self.connection.rollback()
                raise e

        return execute

    return decorator
