#!/usr/local/bin/perl

use LWP::Simple;
use HTML::LinkExtor;
use Data::Dumper;
use URI;
use WWW::Mechanize;
use URI::Escape;

die "Usage spider.pl url" unless (defined $ARGV[0]);
my $url = URI->new(shift @ARGV);
my $domain = $url->host;
my $content = get($url->canonical);
die "Failed to retrieve url" if (!defined $content);

my $mech = WWW::Mechanize->new();
$mech->get($url);
my @links = $mech->links;
my $content = $mech->content;

processContent($url, $content);
depthFirst(@links);

sub depthFirst
{
	my @links = $_[0];
	foreach (@links) {
		my $link = $_->url;
		my $url = URI->new($link);
		
		print $url->host . " " . $domain . "\n";
		if (URI::eq($url->host, $domain)) {
			$mech->get($url->canonical);
			my @links = $mech->links;
			my $content = $mech->content;
			processContent($url, $content);
			depthFirst(@links, $parser);
		}
	}
}

sub processContent
{
	my $url = $_[0];
	my $content = $_[1];
	my $filename = $domain . uri_escape($url);
	open(MYFILE, $filename);
	print MYFILE "$content";	
	close(MYFILE);
}
