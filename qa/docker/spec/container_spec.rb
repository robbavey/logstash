require "serverspec"
require_relative 'spec_helper'


compatible_image_flavors.each do | flavor|
  describe "Running container - #{flavor}" do
    before(:all) {
      set :backend, :docker
      @image = find_image(flavor)

      if flavor == 'full'
        env = {'ENV' => ['xpack.monitoring.elasticsearch.password="hithere"']}
      else
        env = {}
      end
      @container = start_container(@image, env)
      set :docker_container, @container.id
    }

    after(:all) do
      cleanup_container(@container)
      Specinfra::Configuration.instance_variable_set("@docker_container", nil)
      Specinfra::Configuration.instance_variable_set("@docker_image", nil)
      Specinfra::Backend::Docker.clear
      set :backend, :exec
    end

    context command('curl -s http://localhost:9600/_node/stats') do
      its(:stdout) { should match /workers/ }
    end

    context process("java") do
      it { should be_running }
      its(:user) { should eq 'logstash' }
      its(:group) { should eq 'logstash' }
      its(:args) { should contain "-Dls.cgroup.cpu.path.override=" }
      its(:args) { should contain "-Dls.cgroup.cpuacct.path.override=" }
      its(:pid) { should eq 1 }
    end
  end
end
