package com.transactor.serialization

import scala.util.Try

trait SerDe[In, +S] {
    def serialize(in: In): S

    def deserialize(s: S): In
}

type SerializedJson = String

