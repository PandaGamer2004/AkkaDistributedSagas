package com.transactor.serialization

import scala.util.Try

trait SerDe[In, S] {
    def serialize(in: In): Try[S]

    def deserialize(s: S): Try[In]
}

type SerializedJson = String

