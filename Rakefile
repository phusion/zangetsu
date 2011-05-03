task :default => :native_support

task :test => :native_support do
	sh "spec -f s -c test/*_spec.rb"
end

task :clean do
	sh "rm -rf lib/zangetsu/native_support.node ext/build ext/.lock-wscript"
end

task :native_support => ['lib/zangetsu/native_support.node']

file 'lib/zangetsu/native_support.node' => 'ext/build/default/native_support.node' do
	sh "ln -sf ../../ext/build/default/native_support.node lib/zangetsu/native_support.node"
end

file 'ext/build/default/native_support.node' => ['ext/native_support.cpp', 'ext/build'] do
	sh "cd ext && node-waf build"
end

file 'ext/build' => 'ext/wscript' do
	sh "cd ext && node-waf configure"
end