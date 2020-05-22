from reprlib import Repr


class LimitedRepr(Repr):

    def __init__(self, length):
        super().__init__()
        self.maxstring = self.maxother = length


# With length=200 the enclosed type in Envelope in always visible
lrepr = LimitedRepr(length=200).repr
