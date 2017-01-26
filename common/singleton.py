#-*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    @classmethod
    def reset(cls, clazz):
        """Resets the Singleton instance for the given class

        :param clazz: the class to reset
        """
        cls._instances.pop(clazz, None)