#pragma once

namespace gollum {

struct SerExecutionTag { };
inline constexpr SerExecutionTag ser_execution;

struct ParExecutionTag { };
inline constexpr ParExecutionTag par_execution;

}