from dataclasses import field, MISSING


class ExpressDataclassMeta(type):
    def __new__(cls, name, bases, attrs):
        # 새로운 속성 사전 생성
        new_attrs = {k: v for k, v in attrs.items()}

        # dataclass의 필드들을 처리
        annotations = new_attrs.get('__annotations__', {})
        for field_name, field_type in annotations.items():
            if not field_name.startswith('_'):
                # 변수명이 언더스코어로 시작하지 않으면 언더스코어를 붙인 이름으로 변경
                prop_name = f"_{field_name}"
                actual_field_name = field_name

                # 기본값이 있는 필드의 경우, 기본값을 유지
                default = attrs.get(field_name, MISSING)
                if default is MISSING:
                    new_attrs[prop_name] = field(default=None)
                else:
                    new_attrs[prop_name] = default

                # getter와 setter 생성
                new_attrs[actual_field_name] = cls.create_property(prop_name)

        # 새로운 클래스 생성
        return super().__new__(cls, name, bases, new_attrs)

    @staticmethod
    def create_property(prop_name):
        def getter(self):
            return getattr(self, prop_name)

        def setter(self, value):
            setattr(self, prop_name, value)

        return property(getter, setter)
