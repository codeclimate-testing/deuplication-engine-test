# pylint: enable=missing-docstring
"""API for dealing with deferred tasks."""
 
import datetime
import json
import logging
import os
import typing
import uuid
 
import six
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_psql
import sqlalchemy.sql as sa_sql
 
import wp.cache
import wp.dispatch
import wp.instrumentation
import wp.signals
import wp.tasks
from wp import constants
from wp import db
from wp import models
from wp import util
from wp.tasks import priority as task_priority
 
_log = logging.getLogger(__name__)
 
 
# Alphabetized list of task queue names, for use in admin.
TASK_NAMES = [
    'background',
    'process_sales_order',
]
 
 
class TaskNotFoundError(Exception):
    """Failed to locate a specified task."""
 
 
class Task:
    """A plain object representing a task from the task table"""
 
    def __init__(self, model):
        """
        :arg model: A `wp.models.Task` instance.
        """
        assert isinstance(model, models.Task)
        self.id = model.id
        self.created = model.created
        self.name = model.name
        self.priority = model.priority
        self.num_attempts = model.num_attempts
        self.max_attempts = model.max_attempts
        self.is_processed = model.is_processed
        self.work = model.work
        self.locked = model.locked
        self.locked_until = model.locked_until
        self.locked_by_name = model.locked_by_name
        self.locked_by_host = model.locked_by_host
        self.locked_by_ip_address = model.locked_by_ip_address
        self.locked_by_pid = model.locked_by_pid
        self.processed = model.processed
        self.elapsed_seconds = model.elapsed_seconds
        self.last_error = model.last_error
        self.memo = model.memo
 
    @property
    def status(self):
        """
        The logical status of this task based on the data about the task.
        """
 
        if self.is_processed:
            return constants.TaskStatus['processed']
 
        now = datetime.datetime.now()
 
        if self.num_attempts < self.max_attempts and self.locked_until > now and not self.locked:
            return constants.TaskStatus['delayed']
 
        if self.num_attempts < self.max_attempts and self.locked_until <= now:
            return constants.TaskStatus['pending']
 
        if self.locked_until > now and self.locked:
            return constants.TaskStatus['locked']
 
        return constants.TaskStatus['abandoned']
 
 
def _filter_query_for_status(query, status_id):
 
    if status_id == constants.TaskStatus.delayed:
        # first attempt will be in the future
        return query.filter(
            models.Task.is_processed.is_(False),
            models.Task.num_attempts < models.Task.max_attempts,
            models.Task.locked_until > sa_sql.func.now(),
            models.Task.locked.is_(None),
        )
 
    if status_id == constants.TaskStatus.pending:
        return query.filter(
            models.Task.is_processed.is_(False),
            models.Task.num_attempts < models.Task.max_attempts,
            models.Task.locked_until <= sa_sql.func.now(),  # not currently locked
        )
 
    if status_id == constants.TaskStatus.locked:
        return query.filter(
            models.Task.is_processed.is_(False),
            models.Task.locked_until > sa_sql.func.now(),
            models.Task.locked.isnot(None),
        )
 
    if status_id == constants.TaskStatus.processed:
        return query.filter(
            models.Task.is_processed.is_(True),
        )
 
    if status_id == constants.TaskStatus.abandoned:
        # not processed, not locked, and ran out of attempts
        return query.filter(
            models.Task.is_processed.is_(False),
            models.Task.num_attempts >= models.Task.max_attempts,
            models.Task.locked_until < sa_sql.func.now(),
        )
 
    raise ValueError("Unknown task status ID '{}'".format(status_id))
 
 
def get_task(task_id):
    """
    Fetch a task by its id, or raise TaskNotFoundError if no task exists with that id.
    """
    model = db.session().query(models.Task).get(task_id)
 
    if not model:
        raise TaskNotFoundError(task_id)
 
    return Task(model)
 
 
def find_tasks(offset=0, limit=None, name=None, work_filter=None, status_id=None):
    """
    Return a list of `Task` objects matching some combination of the
    following keyword arguments:
    """
    q = db.session().query(models.Task)
 
    if name:
        q = q.filter_by(name=name)
 
    if work_filter:
        q = q.filter(sa.cast(models.Task.work, sa.String).ilike("%{}%".format(work_filter)))
 
    if status_id:
        q = _filter_query_for_status(q, status_id)
 
    filtered_count = q.count()
    q = q.order_by(models.Task.id.desc()).offset(offset)
 
    if limit is not None:
        q = q.limit(limit)
    return util.CountedResult(
        filtered_count=filtered_count,
        results=[Task(task) for task in q.all()],
    )
 
 
def get_task_velocity(task_name=None):
    """
    Return the number of tasks that have completed in the last minute.
    Optionally, can be filtered by task name.
    """
    delta = datetime.timedelta(minutes=1)
    q = db.session().query(sa_sql.func.count(models.Task.id))
    q = q.filter(models.Task.processed >= sa_sql.func.now() - delta)
    if task_name:
        q = q.filter_by(name=task_name)
 
    return q.scalar()
 
 
def get_pending_task_count(task_name):
    """
    Return the number of pending tasks by task name. This matches the query
    used by `lock_task` to find the next available task.
    """
    q = db.session().query(sa_sql.func.count(models.Task.id))
    q = q.filter(models.Task.name == task_name)
    q = _filter_query_for_status(q, constants.TaskStatus.pending)
    return q.scalar()
 
 
CONTEXT_KEY = '_context'
MAX_WORKFLOW_DEPTH = 20
 
Context = typing.Dict[str, typing.Union[str, typing.List[str]]]
 
 
class WorkflowDepthError(Exception):
    """
    Raised when a task is enqueued past the maximum workflow depth.
 
    To avoid creating arbitrarily deep chains of tasks, we put a hard
    limit on the workflow depth, as defined by `wp.cases.task.MAX_WORKFLOW_DEPTH`.
    To model tasks that kick off more of themselves with a workflow, prefer
    `add_workflow_sibling_task`.
    """
 
 
class NonWorkflowTaskError(Exception):
    """
    Raised when a workflow task is expected but not found.
    """
 
 
def get_new_workflow_context() -> Context:
    """
    Create a new workflow context to be used by `add_workflow_subtask`.
 
    Callers should generally prefer calling `add_new_workflow_task` which
    generates this context automatically, however this function may be
    useful to associate many top-level tasks to the same workflow.
    """
    return {
        'workflow_id': str(uuid.uuid4()),
        'call_path': [],
    }
 
 
def add_new_workflow_task(fn, *, args=None, kwargs=None, max_attempts=constants.TASK_MAX_ATTEMPTS,
                          priority=task_priority.DEFAULT_TASK_PRIORITY, defer_seconds=None):
    """
    Create a new task to run the specified function in the background,
    as the first of a new workflow of tasks.
 
    The args are the same as `add_background_task`, but `fn` should accept
    a keyword arg `_context`, which is injected into `kwargs` (manually
    passing `_context` to kwargs is a ValueError). This `_context` is
    strictly for maintaining the workflow; notably, task functions must
    not try to inspect `_context`. Should this task need to kick off
    tasks of its own, it should call `add_workflow_subtask` with this
    `_context` object as its `context` parameter.
    """
    return _add_workflow_task(
        fn=fn,
        args=args,
        kwargs=kwargs,
        priority=priority,
        context=get_new_workflow_context(),
        defer_seconds=defer_seconds,
        max_attempts=max_attempts,
    )
 
 
def add_workflow_subtask(fn, *, context, args=None, kwargs=None,
                         priority=task_priority.DEFAULT_TASK_PRIORITY, defer_seconds=None,
                         max_attempts=constants.TASK_MAX_ATTEMPTS):
    """
    Create a new task to run the specified function in the background,
    as a component of an existing workflow.
 
    The args are the same as `add_new_workflow_task`, with the addition
    of a `context` argument, which should be passed as the `_context`
    parameter of the task function calling `add_workflow_subtask`.
    """
    return _add_workflow_task(
        fn=fn,
        args=args,
        kwargs=kwargs,
        priority=priority,
        context=context,
        defer_seconds=defer_seconds,
        max_attempts=max_attempts,
    )
 
 
def add_workflow_sibling_task(fn, *, context, args=None, kwargs=None,
                              priority=task_priority.DEFAULT_TASK_PRIORITY, defer_seconds=None,
                              max_attempts=constants.TASK_MAX_ATTEMPTS):
    """
    Create a new task to run the specified function in the background,
    sibling to the current workflow task.
 
    The args are the same as `add_workflow_subtask`. Prefer this function
    to `add_workflow_subtask` for task functions that enqueue more tasks
    of the same function, to avoid an unbounded call path depth.
    """
    if not context['call_path']:
        # This can occur when `context` was generated explicitly by
        # `get_new_workflow_context`. In this case, prefer adding the task
        # with `add_workflow_subtask` (or simply `add_new_workflow_task`)
        # instead.
        raise NonWorkflowTaskError("Cannot add a sibling task from non-workflow")
 
    # Get the parent context
    context_clone = dict(context, call_path=context['call_path'][:-1])
 
    return _add_workflow_task(
        fn=fn,
        args=args,
        kwargs=kwargs,
        priority=priority,
        context=context_clone,
        defer_seconds=defer_seconds,
        max_attempts=max_attempts,
    )
 
 
def _add_workflow_task(fn, *, args, kwargs, priority, context, defer_seconds, max_attempts):
    """
    Create a new task to run the specified function in the background,
    with tooling to track the task workflow.
 
    See `add_new_workflow_task` for more information about workflows.
    """
    kwargs = kwargs or {}
    if CONTEXT_KEY in kwargs:
        raise ValueError(f"Keyword arg {CONTEXT_KEY!r} is reserved.")
 
    if len(context['call_path']) >= MAX_WORKFLOW_DEPTH:
        raise WorkflowDepthError
 
    # need to shallow-clone `context` to shallow-mutate it
    context = kwargs[CONTEXT_KEY] = dict(context)
 
    fn_string = fn_to_string(fn)
    context['call_path'] = [*context['call_path'], fn_string]
    payload = {
        'fn': fn_string,
        'args': args,
        'kwargs': kwargs,
        'workflow_id': context['workflow_id'],
        'call_path': context['call_path'],
    }
    return add_task(
        name='background',
        work=payload,
        priority=priority,
        defer_seconds=defer_seconds,
        max_attempts=max_attempts,
        session=wp.db.session(replica_safe=False),
    )
 
 
def add_background_task(fn, args=None, kwargs=None, priority=task_priority.DEFAULT_TASK_PRIORITY,
                        defer_seconds=None, max_attempts=constants.TASK_MAX_ATTEMPTS):
    """
    Create a new task to run the specified function in the background.
    The ``args`` argument must be a Python value that can safely be
    serialized as JSON, as any values that don't have a standard JSON
    representation will be encoded using the ``str`` built-in function.
 
    :arg callable/str fn: The task callable to be invoked.
    :arg args: A list that can be serialized as JSON. This
        will be passed to the callable when it is invoked.
    :arg kwargs: A dict of the function's keyword arguments. Each kwarg
        should be a scalar value, as some transformation happens on them:
 
            In [7]: x = ('hello', 'world', 17)
 
            In [8]: y = json.dumps(x)
 
            In [9]: z = json.loads(y)
 
            In [10]: type(x), type(z), y
            Out[10]: (tuple, list, '["hello", "world", 17]')
 
    :arg int priority: The task's priority.
    :arg int defer_seconds: If a positive int, the task will not become
        available for processing for this many seconds.
    :returns: The new task's id.
    """
    payload = {
        'fn': fn_to_string(fn),
        'args': args,
        'kwargs': kwargs,
    }
    return add_task(
        name='background',
        work=payload,
        priority=priority,
        defer_seconds=defer_seconds,
        max_attempts=max_attempts,
        session=wp.db.session(replica_safe=False),
    )
 
 
def fn_to_string(fn):
    """
    Given a callable or string representing a fully-qualified function name,
    return the fully-qualified function name.
 
    :arg callable/str fn: The function to be stringified, or a string
    :returns: String representing the fully-qualified function name
    """
    return (
        fn
        if isinstance(fn, six.string_types)
        else "{module}.{fn_name}".format(module=fn.__module__, fn_name=fn.__name__)
    )
 
 
def add_task(name, work, priority=task_priority.DEFAULT_TASK_PRIORITY, defer_seconds=None,
             max_attempts=constants.TASK_MAX_ATTEMPTS, session=None):
    """
    Create a new task, returning its id. The ``work`` argument must be a
    Python value that can safely be serialized as JSON, as any values
    that don't have a standard JSON representation will be encoded using
    the ``str`` built-in function.
 
    :arg str name: The task name, indicates a category of tasks.
    :arg work: Some Python value that can be serialized as JSON.
    :arg int priority: The task's priority.
    :arg int defer_seconds: If a positive int, the task will not become
        available for processing for this many seconds.
    :arg int max_attempts: Maximum number of failed attempts before the
        task will stop trying to rerun.
    :arg session: If a database session is not provided, the task will
        be created inside of a `wp.db.session_scope` context, causing
        the session to be committed or rolled back, then closed. If a
        session is provided, then the task will be added, and the
        session will be flushed, but no other session management will
        be done.
    :returns: The new task's id.
    """
    # Ensure the work object is JSON serializable, and therefore won't
    # be rejected by SQLAlchemy, by first marshaling it using 'str' as
    # the default encoder, then unmarshalling it back to a Python value.
    work = json.loads(json.dumps(work, default=str))
 
    def _add_task(_session):
        task = models.Task(name=name, work=work, priority=priority, max_attempts=max_attempts)
        if defer_seconds is not None and defer_seconds > 0:
            task.locked_until = sa_sql.func.now() + datetime.timedelta(seconds=defer_seconds)
        _session.add(task)
        _session.flush()
        return task.id
 
    # Only use session_scope if a session was not provided, to avoid
    # commit/rollback/close of the global session in cases where the
    # session is handled elsewhere (e.g. in request handlers).
    with _statsd(name=name).timer('created'):
        if session is None:
            with db.session_scope() as scoped_session:
                task_id = _add_task(scoped_session)
        else:
            task_id = _add_task(session)
 
    return task_id
 
 
def lock_task(agent_name, name, lock_seconds=10*60, respect_priority=True,
              allowed_tasks=None, denied_tasks=None):
    """
    Lock the next available task and return its id. If no tasks were
    available, return ``None``.
 
    :arg str agent_name: Name of the agent. Used for informational
        purposes only, to describe the holder of the lock.
    :arg str name: The type to task to lock.
    :arg int lock_seconds: The number of seconds to hold the lock. This
        should be longer than the expected time to process any task of
        this type.
    :arg bool respect_priority: If true, tasks are processed in order of
        priority (low to high), then age (oldest to newest). Otherwise,
        the order is arbitrary.
    :arg allowed_tasks: A list of work 'fn's that should only be executed
    :arg denied_tasks: A list of work 'fn's that should not be executed
    :returns: The locked task id, or ``None``.
    """
    with _statsd(name=name).timer('locked'), db.session_scope() as session:
 
        # Find and lock the next available task in the set.
        q = session.query(models.Task.id)
        q = q.filter(models.Task.name == name)
        if allowed_tasks:
            q = q.filter(models.Task.work['fn'].astext.in_(allowed_tasks))
        if denied_tasks:
            q = q.filter(~models.Task.work['fn'].astext.in_(denied_tasks))
        q = _filter_query_for_status(q, constants.TaskStatus.pending)
        if respect_priority:
            q = q.order_by(models.Task.priority, models.Task.locked_until)
        q = q.limit(1).with_for_update(skip_locked=True)
 
        task_id = q.scalar()
 
        if not task_id:
            return None
 
        # Mark the row as locked for our exclusive use.
        locked_until = sa.func.now() + datetime.timedelta(seconds=lock_seconds)
        session.query(models.Task).filter_by(id=task_id).update({
            'num_attempts': models.Task.num_attempts + 1,
            'locked_until': locked_until,
            'locked': sa.func.now(),
            'locked_by_name': agent_name[:100],
            'locked_by_host': util.get_hostname()[:100],
            'locked_by_ip_address': util.get_ip_address(),
            'locked_by_pid': os.getpid(),
        }, synchronize_session=False)
 
    return task_id
 
 
def _finish_task_values(memo, elapsed_seconds=None):
    """
    Get values to update when finishing tasks.
    """
    values = {
        'is_processed': sa_sql.true(),
        'processed': sa_sql.func.now(),
        'locked_until': sa_sql.func.now(),
    }
    if elapsed_seconds is not None:
        # we store milliseconds here
        values['elapsed_seconds'] = elapsed_seconds * 1000
    if memo is not None:
        # Ensure the memo object is JSON serializable, and therefore won't
        # be rejected by SQLAlchemy, by first marshaling it using 'str' as
        # the default encoder, then unmarshalling it back to a Python value.
        values['memo'] = json.loads(json.dumps(memo, default=str))
    return values
 
 
def finish_task(task_id, memo=None, elapsed_seconds=None):
    """
    Indicate that a processor successfully processed a task.
 
    :arg int task_id: The id originally returned by `lock_task`.
    :arg dict memo: Any information that is returned from processing a task.
    :arg float elapsed_seconds: Seconds spent processing this task.
    """
    values = _finish_task_values(memo=memo, elapsed_seconds=elapsed_seconds)
    _isolated_task_update(task_id, values)
 
    if elapsed_seconds is not None:
        _statsd(task_id=task_id).timing('finished', elapsed_seconds * 1000)
 
 
def soft_fail_task(task_id, delay_seconds=None, memo=None):
    """
    Indicate that a processor failed to successfully process a task,
    but not because of a notable error.
 
    :arg int task_id: The id originally returned by `lock_task`.
    :arg int delay_seconds: Number of seconds to wait before making this
        task available for processing again. If ``None``, the task won't
        be available until the lock expires (see the ``lock_seconds``
        arg to `lock_task`).
    """
    values = {}
    if delay_seconds is not None:
        values['locked_until'] = sa_sql.func.now() + datetime.timedelta(seconds=delay_seconds)
    if memo is not None:
        values['memo'] = memo
 
    _isolated_task_update(task_id, values)
 
 
def fail_task(task_id, delay_seconds=None, elapsed_seconds=None, error=None, memo=None):
    """
    Indicate that a processor failed to successfully process a task.
 
    :arg int task_id: The id originally returned by `lock_task`.
    :arg int delay_seconds: Number of seconds to wait before making this
        task available for processing again. If ``None``, the task won't
        be available until the lock expires (see the ``lock_seconds``
        arg to `lock_task`).
    :arg int elapsed_seconds: Seconds spent processing this task.
    :arg str error: Any relevant error information that caused the
        processor to fail at this task.
    """
    values = {}
    if delay_seconds is not None:
        values['locked_until'] = sa_sql.func.now() + datetime.timedelta(seconds=delay_seconds)
    if elapsed_seconds is not None:
        values['elapsed_seconds'] = elapsed_seconds
    if error is not None:
        values['last_error'] = error
    if memo is not None:
        values['memo'] = memo
 
    # The task may have failed due to a generic exception,
    # or possibly due to the database session raising an exception.
    # In either case, calling `remove` will close all connections
    # and delete the session from the pool so that a we guarantee a
    # new master session is used when updating the task about the failure.
    db.session().remove()
 
    _isolated_task_update(task_id, values)
 
    if elapsed_seconds is not None:
        _statsd(task_id=task_id).timing('failed', elapsed_seconds * 1000)
 
 
def _restart_task_values():
    """
    Get values to update when restarting tasks.
    """
    return {
        'is_processed': sa_sql.false(),
        'processed': None,
        'max_attempts': models.Task.num_attempts + 1,
    }
 
 
def restart_task(task_id):
    """
    Indicate that a task should be run again.
 
    This does not remove the lock from the task, so calling this while a processor already has
    the task will have no ill effect.
 
    :arg task_id: The id originally returned by `lock_task`.
    """
    values = _restart_task_values()
    _isolated_task_update(task_id, values)
    _statsd(task_id=task_id).incr('restarted')
 
 
def restart_abandoned_tasks(name, fn, memo, memo_query):
    """
    Indicate that all abandoned tasks matching the criteria should be run again.
 
    This does not remove the lock from the task, so calling this while a processor already has
    the task will have no ill effect.
    """
    _isolated_abandoned_tasks_update(
        name=name,
        fn=fn,
        memo=memo,
        values=_restart_task_values(),
        memo_query=memo_query,
    )
 
 
def finish_abandoned_tasks(name, fn, memo, new_memo, memo_query):
    """
    Indicate that a processor successfully processed all abandoned tasks matching the criteria.
    """
    _isolated_abandoned_tasks_update(
        name=name,
        fn=fn,
        memo=memo,
        values=_finish_task_values(memo=new_memo),
        memo_query=memo_query,
    )
 
 
def restart_abandoned_workflow_tasks(workflow_id, call_path):
    """
    Indicate that all abandoned tasks in this workflow with the given
    call_path should be run again.
    """
    _isolated_workflow_task_update(
        workflow_id=workflow_id,
        call_path=call_path,
        task_status=constants.TaskStatus.abandoned,
        values=_restart_task_values(),
    )
 
 
def finish_abandoned_workflow_tasks(workflow_id, call_path, new_memo):
    """
    Indicate that a processor successfully processed all abandoned tasks in this workflow
    with the given call_path.
    """
    _isolated_workflow_task_update(
        workflow_id=workflow_id,
        call_path=call_path,
        task_status=constants.TaskStatus.abandoned,
        values=_finish_task_values(memo=new_memo),
    )
 
 
def _isolated_abandoned_tasks_update(name, fn, memo, values, memo_query):
    """
    Update all abandoned tasks matching the criteria, ensuring any error gets logged.
 
    This is important for cases such as the memo can't be JSON encoded.
    If it weren't for the logging here, the only place the error would
    show up would be in the application logs (harder to debug).
 
    :returns: number of tasks updated
    """
    if not values:
        # It's possible that there is no need to actually update the row,
        # assuming we are content to simply let the lock expire.
        return 0
 
    try:
        with db.session_scope() as session:
            q = (
                session.query(models.Task)
                .filter(models.Task.name == name)
            )
            if fn:
                q = q.filter(models.Task.work['fn'].astext == fn)
            if memo_query:
                q = q.filter(sa.cast(models.Task.memo, sa.TEXT).ilike("%{}%".format(memo_query)))
            else:
                q = q.filter(sa.cast(models.Task.memo, sa_psql.JSONB) == (memo or None))
            q = _filter_query_for_status(q, constants.TaskStatus.abandoned)
            return q.update(values, synchronize_session=False)
    except Exception:
        _log.exception("Error saving the result of a task.")
        raise
 
 
def _isolated_task_update(task_id, values):
    """
    Update a task, ensuring any error gets logged.
 
    This is important for cases such as the memo can't be JSON encoded.
    If it weren't for the logging here, the only place the error would
    show up would be in the application logs (harder to debug).
    """
    if not values:
        # It's possible that there is no need to actually update the row,
        # assuming we are content to simply let the lock expire.
        return
 
    try:
        with db.session_scope() as session:
            q = session.query(models.Task).filter_by(id=task_id)
            q.update(values, synchronize_session=False)
    except Exception:
        _log.exception("Error saving the result of a task.")
        raise
 
 
def _isolated_workflow_task_update(*, workflow_id, call_path, task_status, values):
    """
    Update workflow tasks by call_path, ensuring any error gets logged.
 
    This is important for cases such as the memo can't be JSON encoded.
    If it weren't for the logging here, the only place the error would
    show up would be in the application logs (harder to debug).
 
    :returns: number of tasks updated
    """
    if not values:
        # It's possible that there is no need to actually update the row,
        # assuming we are content to simply let the lock expire.
        return
 
    try:
        with db.session_scope() as session:
            q = (
                session.query(models.Task)
                .filter(models.Task.work['workflow_id'].astext == workflow_id)
                .filter(sa.cast(models.Task.work['call_path'], sa_psql.JSONB) == call_path)
            )
            q = _filter_query_for_status(q, task_status)
            q.update(values, synchronize_session=False)
    except Exception:
        _log.exception("Error saving the result of a task.")
        raise
 
 
def cleanup_tasks(name, seconds):
    """
    Delete processed tasks of a certain name that succeeded before a relative point in the past.
 
    Maintaining past tasks offers no benefit after a certain time period and will simply inflate
    the db. The single purpose of these objects is to keep track of pending work. It is the
    responsibility of the system processing a task to keep any long term logs above and
    beyond what my be stored in these task objects.
 
    Tasks themselves have no self governance on cleanup. Each overseer should control the
    frequency of cleaning and optionally the duration of keeping processed tasks.
 
    :arg str name: The task name, indicates a category of tasks.
    :arg int seconds: Tasks processed less than this many seconds ago will not be touched.
    """
    with _statsd(name=name).timer('cleanup'):
        with db.session_scope() as session:
            q = session.query(models.Task).filter_by(name=name, is_processed=True)
            before_date = datetime.datetime.utcnow() - datetime.timedelta(seconds=seconds)
            q = q.filter(models.Task.processed < before_date)
            q.delete(synchronize_session=False)
 
 
def _statsd(task_id=None, name=None):
    if name is None:
        task = db.session().query(models.Task).get(task_id)
        name = task.name
 
    return wp.instrumentation.statsd_client('tasks.' + name)
 
 
def find_top_abandoned_tasks(*, name, work, memo, group_by_memo, limit, offset=0):
    """
    Get the count of abandoned tasks grouped by work function and memo.
 
    Note that the ordered aggregation of the query prevents the limit
    from helping query performance. The limit is strictly for a more
    accessible output.
    """
    session = wp.db.session()
    fn = models.Task.work['fn'].astext
    memo_jsonb = sa.cast(models.Task.memo, sa_psql.JSONB)
    # We want the TEXT of the memo, but we want to group by JSONB,
    # so we double-cast here.
    memo_text = sa.cast(models.Task.memo, sa.TEXT)
    count = sa.func.count().label('count')
    subq = session.query(
        fn.label('fn'),
        sa.func.first(memo_text).label('memo'),
        sa.func.first(models.Task.id).label('any_task_id'),
        models.Task.name,
        count,
    )
    subq = _filter_query_for_status(subq, constants.TaskStatus.abandoned)
    if name:
        subq = subq.filter(models.Task.name == name)
    if work:
        subq = subq.filter(fn.ilike("%{}%".format(work)))
    if memo:
        subq = subq.filter(sa.cast(memo_jsonb, sa.TEXT).ilike("%{}%".format(memo)))
 
    # In the future, it might make sense to remove this conditional and just do this
    # GROUP BY by default. However, since this hasn't been used in the while yet,
    # it's probably good to include the option of looking at all these tasks individually.
    if group_by_memo and memo:
        subq = subq.group_by(
            fn,
            sa.func.substring(sa.func.lower(memo_text), f"%{memo}%".lower()),
            models.Task.name,
        ).subquery()
    else:
        subq = subq.group_by(fn, memo_jsonb, models.Task.name).subquery()
 
    q = session.query(subq).order_by(subq.c.count.desc()).limit(limit).offset(offset)
    return [
        {
            'fn': row.fn,
            'memo': row.memo,
            'any_task_id': row.any_task_id,
            'count': row.count,
            'name': row.name,
        }
        for row in q
    ]
 
 
def get_task_workflow(workflow_id):
    """
    Get the tree of tasks in a task workflow by their common workflow_id.
    """
    session = wp.db.session()
    fn_col = models.TaskStatus.work['fn'].astext
    call_path_col = sa.cast(models.TaskStatus.work['call_path'], sa_psql.JSONB)
    query = (
        session.query(
            call_path_col.label('call_path'),
            sa.func.first(fn_col).label('fn'),
            sa.func.first(models.TaskStatus.id).label('any_task_id'),
            sa.func.count().filter(models.TaskStatus.status == 'abandoned').label('abandoned'),
            sa.func.count().filter(models.TaskStatus.status == 'processed').label('processed'),
            sa.func.count().filter(models.TaskStatus.status == 'pending').label('pending'),
            sa.func.count().filter(models.TaskStatus.status == 'locked').label('locked'),
            sa.func.count().filter(models.TaskStatus.status == 'delayed').label('delayed'),
            sa.func.count().label('count'),
        )
        .filter(models.TaskStatus.work['workflow_id'].astext == workflow_id)
        .group_by(call_path_col)
    )
    lookup = {
        tuple(row.call_path): {
            'fn': row.fn,
            'count': row.count,
            'any_task_id': row.any_task_id,
            'call_path': row.call_path,
 
            'status_counts': {
                'abandoned': row.abandoned,
                'processed': row.processed,
                'pending': row.pending,
                'locked': row.locked,
                'delayed': row.delayed,
            },
            'children': [],
        }
        for row in query
    }
    if not lookup:
        return None
 
    # Fill gaps
    # Tasks are deleted after ~a day when they process successfully,
    # so we fill in "missing" parent tasks we know about.
    for call_path, task_data in list(lookup.items()):
        parent = call_path[:-1]
        while parent not in lookup:
            lookup[parent] = {
                'fn': parent[-1] if parent else '',
                'call_path': list(parent),
                'count': 0,
                'status_counts': {},
                'children': [],
            }
            parent = parent[:-1]
 
    # Construct task tree
    for call_path, task_data in lookup.items():
        if call_path:
            parent = call_path[:-1]
            lookup[parent]['children'].append(task_data)
    return lookup[()]['children']
 
 
# This currently seems like the most appropriate spot to locate
# this callback function so that it gets properly registered as a
# receiver for handling gauging cache stats for the tasks.
@wp.dispatch.receiver(wp.signals.task_finished)
def cb_task_finished_lru_cache_stats(sender, handler=None, **kwargs):
    """
    Handles providing metrics for our lru caches from our task agents.
 
    NOTE: The `cb_` prefix of this function is to let pylint know that
    this function is a callback, so don't check for unused arguments.
    """
    dogstatsd = wp.instrumentation.dogstatsd_client(
        prefix='lru_cache',
    )
    for func in wp.cache.lru_cached_funcs:
        cache_info = func.cache_info()._asdict()
        tags = [
            f'function:{func.__module__}.{func.__name__}',
            f'function_module:{func.__module__}',
            f'function_name:{func.__name__}',
        ]
        for key, value in cache_info.items():
            # Due to the number of tasks executing, it should still be useful
            # sampling only 10%.
            dogstatsd.gauge(metric=key, value=value, tags=tags, sample_rate=0.1)