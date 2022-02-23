import logging

from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class Event:
    """An Event in a context. This is deliberately minimal.
    The new state is represented as a string, which should make
    sense wrt the other states in this context (and perhaps
    with other unspecified contexts - for example, all task
    contexts should use the same set of states)

    Tooling should not expect the lists of states to be
    defined.

    It might be useful to include a human readable event provenance
    (eg the name of a log file and the line number in that log file)
    to lead users to information about a particular event.

    Time is specified as a unix timestamp. I'm unclear what the
    best representation for time is in this use case, so picking this
    fairly arbitrarily. Of some concern is that timestamps will come
    from multiple different clocks, and those clocks might need
    to be represented in the timestamp (eg with a hostname?) if use
    can be made of that information.
    """

    time: float
    type: str

    def __repr__(self):
        return f"<Event: type={self.type} time={self.time}>"


class Context:
    """Python representation of a DNPC context.

    A Context has a brief human readable name. This name should
    make sense within the containing context, and should be sized to
    be useful as (for example) a graph label. For example "Task 23"
    It might be, given the type field, that the name only needs to
    make sense alongside the type (so name could be "23" if type
    is "parsl.task")

    A context may contain subcontexts.

    A context may be contained in many supercontexts - a context does
    not know about and does not have an enclosing supercontext.

    The object representation stores the containment
    arrows only from super- to sub- contexts.

    A context may content events / state transitions (I'm unclear on the
    vocabulary I want to use there, and on exactly what should be
    represented.

    The type str indicates the kind of events/subcontexts one might expect
    to find inside a context, and indicates which contexts might be
    compared to each other - eg all task contexts in some sense look the same.
    This is, however, schema and definition free. My recommendation is
    to use some name like "parsl.subcomponent.whatever"

    The subcontexts collection should not be directly set or edited - it should
    be maintained by helper methods provided as part of the Context
    implementation.

    A user should not call the Context() constructor directly - instead use
    the new_root_context and get() class methods

    BUG: attributes set on a Context object are not properly aliased

    BUG: id() and __eq__() don't equate two aliased Context objects, which
    probably messes up use in `set`


    """
    type: str
    name: str
    _subcontexts: Dict[str, "Context"]
    _events: List[Event]

    # if aliased is set, this means that we've had an alias set after
    # multiple contexts already exist. This should point to a more
    # canonical Context, and calls that ask this context for information
    # should be forwarded to that more canonical version.
    aliased: Optional["Context"]

    def __init__(self):
        self._subcontexts = {}
        self._events = []
        self.name = "unnamed"
        self.type = "untyped"
        self.aliased = None

    def __repr__(self):
        if self.aliased is None:
            return (f"<Context @{id(self)} {self.type} "
                    f"({self.name}) with "
                    f"{len(self._subcontexts)} subcontexts, "
                    f"{len(self.events)} events>")
        else:
            return f"<Context @{id(self)} aliased to {self.aliased}"

    @classmethod
    def new_root_context(cls):
        return Context()

    def get_context(self, edge_name, type):
        if self.aliased is not None:
            return self.aliased.get_context(edge_name, type)
        edge_name = str(edge_name)
        c = self._subcontexts.get(edge_name)
        if c is not None:
            assert(c.type == type)
            return c
        else:
            c = Context()
            c.type = type
            self._subcontexts[edge_name] = c

            return c

    def most_aliased_context_obj(self):
        """Returns the Context object that represents this context under
        aliasing."""
        if self.aliased is not None:
            return self.aliased.most_aliased_context_obj()
        else:
            return self

    def alias_context(self, edge_name: str, context: "Context"):
        if self.aliased is not None:
            return self.aliased.alias_context(edge_name, context)
        c = self._subcontexts.get(edge_name)
        if c is not None:
            if c is not context:
                # there are two Context objects that this call is declaring
                # represent the same context, so need to alias them.
                # pick one object to be more canonical than the other.
                # but DANGER! both (or either) context objects might
                # already be aliased
                # to other contexts already in which case there's a whole
                # group of # contexts to alias?
                # if so, then all of the interesting stuff is in the two
                # most-aliased contexts, and it is those that need to be
                # aliased
                a = c.most_aliased_context_obj()
                b = context.most_aliased_context_obj()

                # I don't think it matters which of a/b are chosen to be the
                # master vs the aliased object: they're symmetrically roots.
                assert a.aliased is None
                assert b.aliased is None

                # move the contents of b into a

                # TODO: should there be any checking for consistency in this
                # event append? I don't think there are any consistency rules
                # that need validating.
                a._events += b._events

                for k in b.__dict__:
                    if k in ['_subcontexts', '_events', 'name',
                             'type', 'aliased']:
                        # skip implementation details
                        continue
                    if k in a.__dict__:
                        assert a.__dict__[k] == b.__dict__[k]
                    else:
                        a.__dict__[k] = b.__dict__[k]

                # Each subcontext in b now needs to be aliased into a under
                # the same key.
                # Using the alias_context call recursively will ensure that
                # validation rules are preserved.
                for k in b._subcontexts:
                    bsc = b._subcontexts[k]
                    a.alias_subcontext(k, bsc)

                # now forget everything about b
                # use None rather than empty lists/dicts so as to force a
                # runtime error if they're ever referenced again.
                b._events = None
                b._subcontexts = None

                # Do this after subcontexts so as to not affect subcontext
                # aliasing.
                b.aliased = a

        else:
            self._subcontexts[edge_name] = context

    @property
    def subcontexts(self) -> List["Context"]:
        """The subcontexts property is read-only. It should be maintained by
        Context helper methods."""
        if self.aliased is not None:
            return self.aliased.subcontexts
        return [self._subcontexts[k] for k in self._subcontexts]

    @property
    def subcontexts_dict(self) -> List["Context"]:
        """The subcontexts property is read-only. It should be maintained by
        Context helper methods."""
        if self.aliased is not None:
            return self.aliased.subcontexts
        return self._subcontexts


    @property
    def events(self) -> List[Event]:
        """The reference to a list is immutable - the list cannot be replaced
        by a new list. But the list itself is mutable and new events can be
        appended."""
        if self.aliased is not None:
            return self.aliased.events

        return self._events

    def subcontexts_by_type(self, typename) -> List["Context"]:
        # no alias checking here, but self.subcontexts is aliased so
        # the appropriate aliasing happens there.

        return [c for c in self.subcontexts if c.type == typename]

    def select_subcontexts(self, predicate: Callable) -> "Context":
        if self.aliased is not None:
            return self.aliased.select_subcontexts(predicate)

        new_context = Context.new_root_context()
        for key, ctx in self._subcontexts.items():
            if predicate(key, ctx):
                new_context.alias_context(key, ctx)
        return new_context
