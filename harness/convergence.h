// @ Copyright

#include <vector>
#include <functional>

class IConvergenceModel
{
public:
  virtual bool aggregate(const float tail_latency) = 0;
};

using VCFunction = std::function<float(int /* Epoch */)>;

class VariationCoefficientModel : public IConvergenceModel
{
public:
  VariationCoefficientModel(const float vc_conv = 10.f, const int min_samples = 10, const int window = 5);
  VariationCoefficientModel(VCFunction vc_conv, const int min_samples, const int window);

  virtual bool aggregate(const float tail_latency) override;

private:
  const float m_vc_conv;
  const VCFunction m_vc_fun_conv;
  const int m_min_samples;
  const int m_window;
  std::vector<float> tail_latencies;
  int m_epoch;
};

