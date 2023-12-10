// @ Copyright

#include "convergence.h"
#include <algorithm>
#include <cmath>
#include <numeric>
#include <iostream>

static float get_mean(const std::vector<float>& values)
{
  if (values.size() <= 0)
  {
    return 0.f;
  }

  return std::accumulate(values.begin(), values.end(), 0.f) / values.size();
}

// @note Returns the unbiased sample variance
static float get_variance(const std::vector<float>& values)
{
  const std::size_t val_cnt = values.size();
  const float mean = get_mean(values);

  return std::accumulate(values.begin(), values.end(), 0.f, 
    [mean, val_cnt](float acc, float val) -> float
  {
    return acc + ((val - mean) * (val - mean) / (val_cnt - 1));
  });
}

static float get_variation_coefficient(const std::vector<float>& values)
{
  if (values.size() <= 1)
  {
    return 100.f;
  }

  const auto var = get_variance(values);
  const auto mean = get_mean(values);

  if (mean == 0.f)
  {
    if (var == 0.f)
    {
      return 0.f;
    }
    return 100.f;
  }

  return std::sqrt(var) / mean * 100.f;
}

/////////////////////////////////////////////////////////////////////////
// VariationCoefficientModel

VariationCoefficientModel::VariationCoefficientModel(const float vc_conv, const int min_samples, const int window)
  : m_vc_conv(vc_conv)
  , m_min_samples(min_samples)
  , m_window(window)
  , m_epoch(0)
{
}

VariationCoefficientModel::VariationCoefficientModel(VCFunction vc_conv, const int min_samples, const int window)
  : m_vc_conv(-1.f)
  , m_vc_fun_conv(std::move(vc_conv))
  , m_min_samples(min_samples)
  , m_window(window)
  , m_epoch(0)
{
}

bool VariationCoefficientModel::aggregate(const float tail_latency)
{
  tail_latencies.emplace_back(tail_latency);
  ++m_epoch;

  if (m_window)
  {
    tail_latencies = std::vector<float>(tail_latencies.end() - m_window, tail_latencies.end());
  }

  if (m_min_samples > m_epoch)
  {
    return false;
  }

  const float wanted_vc = m_vc_conv <= 0.f ? m_vc_fun_conv(m_epoch) : m_vc_conv;
  
  const auto vc = get_variation_coefficient(tail_latencies);

  std::cout 
    << "Convergence status:\n"
    << " - t_lat: " << tail_latency << "\n"
    << " - mean: " << get_mean(tail_latencies) << "\n"
    << " - var: " << get_variance(tail_latencies) << "\n"
    << " - vc: " << vc << "\n"
    << " - wanted vc < " << wanted_vc << "\n";

  return vc < wanted_vc;
}

