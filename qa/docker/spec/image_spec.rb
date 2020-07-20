require "serverspec"
require_relative 'spec_helper'

compatible_image_flavors.each do | flavor|
  describe "Image definition - #{flavor}" do
    before(:all) do
      image = find_image(flavor)
      set :backend, :docker
      set :docker_image, image.id
    end

    after(:all) do
      Specinfra::Configuration.instance_variable_set("@docker_image", nil)
      Specinfra::Backend::Docker.clear
      set :backend, :exec
    end

    it 'should have the correct version' do
        expect(command('logstash --version').stdout).to match /#{version}/
      end

    context file('cat /usr/share/logstash/LICENSE.txt') do
      its(:content) { should have_correct_license_agreement(flavor)}
    end

    context command('whoami') do
      its(:stdout) { should match /logstash/ }
    end

    it "should have the correct home directory" do
      expect(command('echo $HOME').stdout.chomp).to eq "/usr/share/logstash"
    end

    it "opt/logstash should be a symlink" do
      expect(file('/opt/logstash').symlink?).to be_truthy
      expect(file('/opt/logstash').link_target).to eq "/usr/share/logstash"
    end

    it 'all files should be owned by logstash' do
      expect(command('find /usr/share/logstash ! -user logstash').stdout).to be_empty
    end

    it 'logstash user is uid 1000' do
      expect(command('id -u logstash').stdout.chomp).to eq "1000"
    end

    it 'logstash user is gid 1000' do
      expect(command('id -g logstash').stdout.chomp).to eq "1000"
    end

    it 'should not log to files' do
      expect(file('/usr/share/logstash/config/log4j2.properties').content).not_to match /RollingFile/
    end
  end
end
