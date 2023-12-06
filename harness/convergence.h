// @ Copyright

#include <vector>

class IConvergenceModel
{
public:
  virtual bool aggregate(const float tail_latency) = 0;
};

class VariationCoefficientModel : public IConvergenceModel
{
public:
  VariationCoefficientModel(const float vc_conv = 10.f, const int min_samples = 10, const int window = 5);

  virtual bool aggregate(const float tail_latency) override;

private:
  const float m_vc_conv;
  int m_min_samples;
  const int m_window;
  std::vector<float> tail_latencies;
};


