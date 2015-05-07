#!/usr/bin/env bash

if [ -z "$1" ]
	then
    		echo "No output dir supplied"
  	else 
    		if [ ! -d "$1" ]; then
				echo "$1 dir doesn't exist"
    		else
			echo "removing $1..."
			rm -rf $1
			echo "$1 removed!"
		fi
fi

