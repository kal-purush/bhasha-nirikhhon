from flask import Flask, flash, request, render_template, url_for, redirect, send_from_directory
from werkzeug.utils import secure_filename
from PIL import Image
import tensorflow as tf
import keras
import os
import sys
import logging
import uuid
from pathlib import Path