require 'date'

Gem::Specification.new do |s|
  s.name        = 's3_rotate'
  s.summary     = "AWS S3 upload with rotation mechanism"
  s.description = s.summary
  s.homepage    = 'https://github.com/Whova/s3_rotate'
  s.license     = 'MIT'

  s.version     = '1.2.0'
  s.date        = Date.today.to_s

  s.authors     = ["Simon Ninon"]
  s.email       = 'simon.ninon@gmail.com'

  s.files       = `git ls-files`.split("\n")
  s.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")

  s.required_ruby_version = '>= 2.0.0'

  s.add_dependency 'fog-aws', '~> 3.5.2'
end
