require 'date'

Gem::Specification.new do |s|
  s.name        = 's3_rotate'
  s.version     = '1.0.0'
  s.homepage    = 'https://github.com/Whova/s3_rotate'
  s.date        = Date.today.to_s
  s.summary     = "AWS S3 upload with rotation mechanism"
  s.description = s.summary
  s.authors     = ["Simon Ninon"]
  s.email       = 'simon.ninon@gmail.com'
  s.files       = `git ls-files`.split("\n")
  s.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.license     = 'MIT'
end
