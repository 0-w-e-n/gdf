package core

import (
    "reflect"
)

func TypeOf (v interface{}) string {
    return reflect.TypeOf(v).String()
}
