ROOT = File.expand_path(File.join(File.dirname(__FILE__), '..', '..', '..'))
$LOAD_PATH.unshift File.join(ROOT, 'logstash-core/lib')
FIXTURES_DIR = File.expand_path(File.join("..", "..", "fixtures"), __FILE__)

require 'logstash/version'
require 'json'
require 'stud/try'
require 'docker-api'

class QuietTry < Stud::Try
  def log_failure(exception, fail_count, message)
    if (fail_count == 1)
      puts "Failed (#{exception})"
    else
      print "."
    end
  end
end

def quiet_try
  @quiet_try ||= QuietTry.new
end

def version
  @version ||= LOGSTASH_VERSION
end

def qualified_version
  qualifier = ENV['VERSION_QUALIFIER']
  qualified_version = qualifier ? [version, qualifier].join("-") : version
  ENV["RELEASE"] == "1" ? qualified_version : [qualified_version, "SNAPSHOT"].join("-")
end

def find_image(flavor)
  Docker::Image.all.detect{
      |image| image.info['RepoTags'].detect{
        |tag| tag == "docker.elastic.co/logstash/logstash-#{flavor}:#{qualified_version}"
    }}
end

def start_container(image, options)
  container = image.run(nil, options)
  quiet_try.try(40.times, RSpec::Expectations::ExpectationNotMetError) do
    expect(container.exec(['curl', '-s', 'http://localhost:9600/_node'])[0][0]).not_to be_empty
  end
  container
end

def cleanup_container(container)
  container.kill
  container.delete(:force=>true)
end

def license_label_for_flavor(flavor)
  flavor.match(/oss/) ? 'Apache 2.0' : 'Elastic License'
end

def license_agreement_for_flavor(flavor)
  flavor.match(/oss/) ? 'Apache License' : 'ELASTIC LICENSE AGREEMENT!'
end

def get_node_info
  JSON.parse(@container.exec(['curl', '-s', 'http://localhost:9600/_node'])[0][0])
end

def get_node_stats
  JSON.parse(@container.exec(['curl', '-s', 'http://localhost:9600/_node/stats'])[0][0])
end

def get_settings
  YAML.load(@container.read_file('/usr/share/logstash/config/logstash.yml'))
end

def compatible_image_flavors
  #is_aarch64? ? %w(aarch64-full aarch64-oss) : %w(full oss)
  %w(full oss)
end

def image_flavors
  #%w(aarch64-full full aarch64-oss oss)
  %w(full oss)
end

def architecture_for_flavor(flavor)
  flavor.match(/aarch64/) ? 'arm64' : 'amd64'
end

def is_aarch64?
  RbConfig::CONFIG["host_os"] == "aarch64"
end

def is_oss?(flavor)
  flavor.match(/oss/)
end

RSpec::Matchers.define :have_correct_license_label do |expected|
  match do |actual|
    values_match? license_label_for_flavor(expected), actual
  end
  failure_message do |actual|
    "expected License:#{actual} to eq #{license_label_for_flavor(expected)}"
  end
end

RSpec::Matchers.define :have_correct_license_agreement do |expected|
  match do |actual|
    values_match? /#{license_agreement_for_flavor(expected)}/, actual
    true
  end
  failure_message do |actual|
    "expected License Agreement:#{actual} to contain #{license_agreement_for_flavor(expected)}"
  end
end