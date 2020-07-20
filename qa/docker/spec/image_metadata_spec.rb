require_relative 'spec_helper'

image_flavors.each do | flavor|
  describe "Image metadata - #{flavor}" do

    before(:all) do
      @image = find_image(flavor)
      @image_config = @image.json['Config']
      @labels = @image_config['Labels']
    end

    it 'creates image' do
      expect(@image).not_to be_nil
    end

    it 'sets the correct working directory' do
      expect(@image_config['WorkingDir']).to eq '/usr/share/logstash'
    end

    it 'sets the correct Architecture' do
      expect(@image.json['Architecture']).to eq architecture_for_flavor(flavor)
    end

    %w(license org.label-schema.license org.opencontainers.image.licenses).each do |label|
      it "sets the license label #{label} correctly" do
        expect(@labels[label]).to have_correct_license_label(flavor)
      end
    end

    %w(org.label-schema.name org.opencontainers.image.title).each do |label|
      it "sets the name label #{label} correctly" do
        expect(@labels[label]).to eq "logstash"
      end
    end

    %w(org.opencontainers.image.vendor).each do |label|
      it "sets the vendor label #{label} correctly" do
        expect(@labels[label]).to eq "Elastic"
      end
    end

    %w(org.label-schema.version org.opencontainers.image.version).each do |label|
      it "sets the version label #{label} correctly" do
        expect(@labels[label]).to eq qualified_version
      end
    end
  end
end
