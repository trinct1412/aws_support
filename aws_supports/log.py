from functools import wraps, partial
from inspect import signature


def event_logger(func=None, tracks=(), logger=None):
    if func is None:
        return partial(event_logger, tracks=tracks)

    @wraps(func)
    def execute(*args, **kwargs):
        is_success = True
        try:
            parameters = list(signature(func).parameters.keys())
            list_params = args if args else []
            dict_params = kwargs if kwargs else {}
            parameters = {**{parameters[i]: k for i, k in enumerate(list_params)}, **dict_params}
            if parameters.get('current_user'):
                parameters['user_id'] = parameters.get('current_user').id

            default_tracks = ['tenant_id', 'user_id', 'device_id']
            tracks_ = list(tracks)
            tracks_.extend(default_tracks)
            parameters = {track_: parameters.get(track_) for track_ in tracks_ if parameters.get(track_)}
            try:
                logger.append_keys(**parameters)
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                is_success = False
                logger.warning(f"exception : {e}")
            finally:
                logger.remove_keys(parameters)
        except Exception as e:
            if not is_success:
                result = func(*args, **kwargs)
                return result
    return execute
