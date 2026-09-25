"""调度服务向调用方暴露的稳定错误。"""


class SchedulingError(RuntimeError):
    code = "scheduling_error"
    status = 400


class Conflict(SchedulingError):
    code = "conflict"
    status = 409


class CapacityExceeded(Conflict):
    code = "capacity_exceeded"
    status = 409
