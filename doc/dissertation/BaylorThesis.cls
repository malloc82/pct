\NeedsTeXFormat{LaTeX2e}[1995/12/01]
\ProvidesClass{BaylorThesis}[2017/06/17]

\typeout{}
\typeout{===============================================================================}
\typeout{BAYLOR UNIVERSITY}
\typeout{Department of Psychology and Neuroscience}
\typeout{Ph.D. Dissertation LaTeX style file}
\typeout{Created by Stephanie Powers--Edited by Alex Beaujean[2013/03/15, 2014/07/05, 2014/10/20, 2017/06/17]}
\typeout{}
\typeout{===============================================================================}

%%
%% We'll let report handle some of the standard options.
%%

\DeclareOption{fleqn}{ \PassOptionsToClass{fleqn}{report} }
\DeclareOption{leqno}{ \PassOptionsToClass{leqno}{report} }
\DeclareOption{openbib}{ \PassOptionsToClass{openbib}{report} }
\DeclareOption{draft}{ \PassOptionsToClass{draft}{report} }
\DeclareOption{final}{ \PassOptionsToClass{final}{report} }
\DeclareOption{openright}{ \PassOptionsToClass{openright}{report} }
\DeclareOption{openany}{ \PassOptionsToClass{openany}{report} }

%%
%% Most of the usual report options are now illegal.
%%

\DeclareOption{10pt}{\ClassWarning{thesis}{Option '10pt' ignored.}}
\DeclareOption{11pt}{\ClassWarning{thesis}{Option '11pt' ignored.}}
\DeclareOption{12pt}{\ClassWarning{thesis}{Option '12pt' ignored (redundant).}}

\DeclareOption{landscape}{\ClassWarning{thesis}{Option 'landscape' ignored.}}
\DeclareOption{twoside}{\ClassWarning{thesis}{Option 'twoside' ignored.}}
\DeclareOption{twocolumn}{\ClassWarning{thesis}{Option 'twocolumn' ignored.}}
\DeclareOption{legalpaper}{\ClassWarning{thesis}{Option 'legalpaper' ignored.}}
\DeclareOption{executivepaper}{\ClassWarning{thesis}{Option 'executivepaper' ignored.}}
\DeclareOption{letterpaper}{\ClassWarning{thesis}{Option 'letterpaper' ignored (redundant).}}
\DeclareOption{onecolumn}{\ClassWarning{thesis}{Option 'onecolumn' ignored (redundant).}}
\DeclareOption{oneside}{\ClassWarning{thesis}{Option 'oneside' ignored (redundant).}}

% Invalid options - normally used in report.cls and others, but not here:

\DeclareOption{titlepage}{\ClassWarning{thesis}{Title page always generated. Option ignored.}}
\DeclareOption{notitlepage}{\ClassWarning{thesis}{Option 'notitlepage' not available for thesis.}}
\DeclareOption{a4paper}{\ClassWarning{thesis}{Option 'a4paper' not available for this class.}}
\DeclareOption{a5paper}{\ClassWarning{thesis}{Option 'a5paper' not available for this class.}}
\DeclareOption{b5paper}{\ClassWarning{thesis}{Option 'b5paper' not available for this class.}}


%%
%% Thesis style is based on the report style.
%%

\ProcessOptions
\LoadClass[12pt,letterpaper,oneside,onecolumn]{report}[1996/10/31]

% Other required LaTeX2e packages: if your computer doesn't already
% have these, you can copy them to your own directory:


\RequirePackage{indentfirst}        % indentfirst.sty, 1995/11/23
\RequirePackage{ifthen}         % ifthen.sty, 1996/08/02
\RequirePackage[titles]{tocloft}		% for formatting the table of contents, list of figures, and list of tables
% \RequirePackage[ULforem,normalbf]{ulem}   % ulem.sty, 1995/12/17

% to spell out chapter numbers
\RequirePackage{fmtcount}


% CUSTOM OPTIONS

% Options for no decimal numbering, or decimal number nested over 3 deep:
\newif\if@Section
\@Sectionfalse



% SPECIAL INTERNAL COMMANDS.  Needed by later routines.

% Create a command to be used for default arguments.
\newcommand*{\th@DefArg}{aWfdRvV11f66GL00K5}

% Now create a command to check for the default argument, and call the
% command [in the first arg] appropriately.  Used for section heading
% commands.

\newcommand{\th@ExecChkOptArg}[3]{
\ifthenelse{\equal{\th@DefArg}{#2}}{
    #1{#3}
    }{
    #1[#2]{#3}}
}

% Convert the given number to a corresponding number name.
% modified by Daniel to make it work with Appendix
\newcommand{\arabicText}[1]{
\ifthenelse { \(\not \equal{#1}{A}\) \and \(\not \equal{#1}{B}\) %
        \and \(\not \equal{#1}{C}\) \and \(\not \equal{#1}{D}\)
        \and \(\not \equal{#1}{E}\) \and \(\not \equal{#1}{F}\)
        \and \(\not \equal{#1}{G}\) \and \(\not \equal{#1}{H}\)
        \and \(\not \equal{#1}{I}\) \and \(\not \equal{#1}{J}\)
        \and \(\not \equal{#1}{K}\) \and \(\not \equal{#1}{L}\) } { %
\ifcase #1 %
  ZERO \or ONE \or TWO \or THREE \or FOUR \or FIVE \or %
  SIX \or SEVEN \or EIGHT \or NINE \or TEN \or ELEVEN \or %
  TWELVE \or THIRTEEN \or FOURTEEN \or FIFTEEN \or SIXTEEN \or %
  SEVENTEEN \or EIGHTEEN \or NINETEEN \or TWENTY %
\fi
}
{#1}
}

% Controls the heading styles.  You can change the size to suit your needs.

\newcommand*{\th@ChSz}{\normalsize}
\newcommand*{\th@ScSz}{\normalsize}
\newcommand*{\th@HdFnt}{\normalfont}

% The style commands proper.  Controlled by a flag.

\newif\if@thInHeading
\@thInHeadingfalse
\newcommand*{\th@ChapterStyle}{\normalsize\normalfont}
\newcommand*{\th@SectionStyle}{\if@thInHeading\th@HdFnt\th@ScSz\fi}
\newcommand*{\th@SubSectionStyle}{\if@thInHeading\normalsize\th@HdFnt\fi}
\newcommand*{\th@OtherSecHeadStyles}{\if@thInHeading\normalsize\th@HdFnt\fi}


\renewcommand\chapter{\if@openright\cleardoublepage\else\clearpage\fi
                    \thispagestyle{plain}%
                    \global\@topnum\z@
                    \@afterindentfalse
                    \secdef\@chapter\@schapter}

\def\@chapter[#1]#2{\ifnum \c@secnumdepth >\m@ne
                         \refstepcounter{chapter}%
                         \typeout{**************************}
                         \typeout{\@chapapp\space\thechapter}%
                         \addcontentsline{toc}{chapter}%
% This was the original syntax, but had to be change to give spelled out numbers                                  {\hskip-.05in\protect\numberline{\rm\thechapter}\rm#1}%
{\hskip-.05in\protect\numberline{\rm\protect\arabicText{\thechapter}}\rm#1}%
                    \else
                      \addcontentsline{toc}{chapter}{\rm#1}%
                    \fi
                    \chaptermark{#1}%
                 	\addtocontents{lof}{\protect\addvspace{10\p@}}%
                    \addtocontents{lot}{\protect\addvspace{10\p@}}%
                    \@makechapterhead{#2}%
                    \@afterheading
                    }
\def\@makechapterhead#1{ 
    \vspace*{0.22in} {  \parindent 0pt \centering
     \MakeUppercase{\@chapapp{}} \hskip-.05in \arabicText{\thechapter}
     \\  #1  \\ 
% Removed the space after the chapter title because added triple space before all section headings 
%\vspace{0.22in} 
} }


\def\@schapter#1{\if@twocolumn
                   \@topnewpage[\@makeschapterhead{#1}]%
                 \else
                   \@makeschapterhead{#1}%
                   \@afterheading
                 \fi}
\def\@makeschapterhead#1{%
  \vspace*{0pt}%
  {\parindent \z@ \raggedright
    \normalfont
    \interlinepenalty\@M
    \Huge \bfseries  #1\par\nobreak
    \vskip 40\p@
  }}

% Makes the chapter heading label, putting it in the desired style.
% We need to define it here, since we need it before redefining the
% chapter commands.

\newcommand*{\th@DoChapCenter}{\centering}
\newcommand*{\th@ChapHead}[1]{
    \protect\th@ChapterStyle
    \protect\th@DoChapCenter #1}


% DEFINE DOUBLE-SPACING (THE DEFAULT) AND OTHER SPACING
\def\setspace@size{\ifx\@currsize\normalsize\@normalsize\else\@currsize\fi}
\def\doublespacing{ \ifcase \@ptsize \relax
      \def \baselinestretch {2.004}%    10pt
  \or \def \baselinestretch {1.771}%    11pt
  \or \def \baselinestretch {1.660}%    12pt
  \fi \setspace@size}
\def\singlespacing{\def \baselinestretch {1} \setspace@size \vskip \baselineskip}
\def\setstretch#1{\renewcommand{\baselinestretch}{#1}}
\def\@setsize#1#2#3#4{\@nomath#1
  \let\@currsize#1 \baselineskip #2
  \baselineskip \baselinestretch\baselineskip
  \parskip \baselinestretch\parskip
  \setbox\strutbox \hbox{ \vrule height.7\baselineskip depth.3\baselineskip
  width\z@} \skip\footins \baselinestretch\skip\footins
  \normalbaselineskip\baselineskip#3#4}


%%%%%%%%% floats & footnotes singlespaced %%%%%%%%%
\let\latex@xfloat=\@xfloat
\def\@xfloat #1[#2]{\latex@xfloat #1[#2]
\def\baselinestretch{1}\@normalsize \normalsize}
\long\def\@footnotetext#1{
  \insert\footins{
    \def\baselinestretch {1}
    \reset@font\footnotesize
    \interlinepenalty\interfootnotelinepenalty
    \splittopskip\footnotesep
    \splitmaxdepth \dp\strutbox \floatingpenalty \@MM
    \hsize\columnwidth \@parboxrestore
    \protected@edef\@currentlabel{
      \csname p@footnote\endcsname\@thefnmark}
    \color@begingroup \@makefntext{
        \rule\z@\footnotesep\ignorespaces#1\@finalstrut\strutbox}
    \color@endgroup}}
\long\def\@mpfootnotetext#1{
  \global\setbox\@mpfootins\vbox{
    \unvbox \@mpfootins
    \def\baselinestretch {1}
    \reset@font\footnotesize
    \hsize\columnwidth \@parboxrestore
    \protected@edef\@currentlabel{
      \csname p@mpfootnote\endcsname\@thefnmark}
    \color@begingroup \@makefntext{
       \rule\z@\footnotesep\ignorespaces#1\@finalstrut\strutbox}
   \color@endgroup}}


%%%%%%%% single / double spacing %%%%%%%%%
\def\singlespace{ \vskip \baselineskip \def\baselinestretch {1}
  \setspace@size \vskip -\baselineskip }
\def\endsinglespace{\par}
\def\spacing#1{ \par \begingroup \def\baselinestretch {#1} \setspace@size }
\def\endspacing{ \par \vskip \parskip \vskip \baselineskip
  \endgroup \vskip -\parskip \vskip -\baselineskip }
\def\doublespace{ \ifcase \@ptsize \relax
      \spacing{2.004}%  10pt
  \or \spacing{1.771}%  11pt
  \or \spacing{1.660}%  12pt
  \fi }
\let\enddoublespace=\endspacing

% SET THE OVERALL DOCUMENT PROPERTIES, such as page numbering, margins...
% Initialize several global document properties, but do so *before*
% the beginning of the document to forces these changes.

% "All typing (except for pagination) and all parts of tables, figures, and appendices must fit within the specified margins: 1.25 inch left and right margins and 1 inch top and bottom margins."


\AtBeginDocument{
        \let\em\it
    \setlength{\topmargin}{.35in}%moves text and number was .295
    \setlength{\headheight}{0in} %was 0.2
    \setlength{\headsep}{0.40625in}%was 0.138in; moves text only
    \setlength{\topskip}{0in}% first line, down from page number; 
                                %moves text only
    \setlength{\textheight}{8.9375in}
    \setlength{\footskip}{.325in}                 %sets length for bottom page nos was .335

    \setlength{\oddsidemargin}{0.25in} % was .55in
    \setlength{\evensidemargin}{0.25in} % .55in

    \setlength{\parindent}{30pt}    % somewhere between 0.3" and 0.5"
    \setlength{\textwidth}{6in}  % was 5.92in
    \setlength{\leftmargini}{3.5em}
    \setlength{\leftmarginii}{2.2em}
    \setlength{\leftmarginiii}{2.2em}
    \setlength{\leftmarginiv}{2.2em}
    \setlength{\leftmarginv}{2.2em}
    \setlength{\leftmarginvi}{2.2em}
    \setlength{\leftmargin}{\leftmargini}
    \setlength{\labelsep}{.5em}
    \setlength{\labelwidth}{ 1.5em}
    \pagenumbering{arabic}
    \pagestyle{myheadings}
    \markright{}
    \onecolumn
    \raggedbottom
    \normalsize
    \normalfont
    \doublespacing  % footnotes and floats still singlespaced
% Try to prevent widows and orphans
\widowpenalty10000
\clubpenalty10000

\newif\ifsecnum
\secnumcall 
\ifsecnum
    \setcounter{secnumdepth}{\value{th@SecNum}}
\else
\setcounter{secnumdepth}{0} %To turn section numbering on, comment this out and uncomment the next like
\fi
   




% Set length of hanging indentation for bibliography entries.
%
%\newlength{\bibhang}
%\setlength{\bibhang}{2em}
% Redefine the bibliography command here, just in case someone
% uses a custom *.sty file that redefines bibliography command
% modified by Daniel to include required hanging indentation
\def\thebibliography#1{\newpage\normalfont \normalsize \centering
\vspace*{0.25in} REFERENCES \vspace*{12pt}
\addcontentsline{toc}{chapter}{\bibnameToC}
\singlespace
 \list
 {
 [\arabic{enumi}]} {\settowidth\labelwidth{[#1]} 
 \leftmargin\labelwidth
\leftmargin\labelsep
\leftmargin\bibindent
 \itemindent -\bibindent
 \listparindent \itemindent
% space between bibliographic entries
\parsep 12pt
% \usecounter{enumi}
}
\def\newblock{\hskip .11em plus .33em minus -.07em}
 \sloppy
 \sfcode`\.=1000\relax
 %\advance\leftmargin\labelsep
 %\usecounter{enumi}} \def\newblock{\hskip .11em plus .33em minus -.07em}
 %\sloppy \sfcode`\.=1000\relax 
 }

%\let\endthebibliography=\endlist

%\newenvironment{Bibliography}{  
%    \begin{thebibliography}{99}
%     }{\end{thebibliography}}
}



% ADDITIONAL STUFF
\newcommand*{\@normalsize}{\normalsize}
\doublespacing

% ==========
% = Floats =
% ==========


% max fraction of floats at top
%\renewcommand*{\topfraction}{.5}
% max fraction of floats at bottom
% \renewcommand{\bottomfraction}{0.8}

%  Parameters for TEXT pages (not float pages):
%\setcounter{topnumber}{1}
%\setcounter{totalnumber}{2}
% \setcounter{bottomnumber}{1}

  % for 2-column pages
  % \setcounter{dbltopnumber}{2}  
% fit big float above 2-col. text
  % \renewcommand{\dbltopfraction}{0.9}	
%\renewcommand*{\dblfloatpagefraction}{.3}

% Text with figures
%\renewcommand*{\textfraction}{.3}

  %   Parameters for FLOAT pages (not text pages):

%\renewcommand*{\floatpagefraction}{.6} 	% N.B.: floatpagefraction MUST be less than topfraction !!
%   \renewcommand{\dblfloatpagefraction}{0.7}


%\setlength{\abovecaptionskip}{0pt}
\setlength{\belowcaptionskip}{2mm}
%\renewcommand*{\intextsep}{1.2cm}
%\renewcommand*{\textfloatsep}{1.2cm}






% Re-define some formatting commands to produce lists, labels,
% and numberings that are more to the Grad School's liking.

% Set up some titles on the table of contents page.
\renewcommand*{\figurename}{\small Figure}
\renewcommand*{\tablename}{\small Table}
\newcommand*{\figurenameToC}{\normalsize\th@HdFnt \normalfont}
\newcommand*{\tablenameToC}{\normalsize\th@HdFnt \normalfont}

% Dots in table of contents, LOF and LOT
\renewcommand*{\cftchapleader}{\cftdotfill{\cftdotsep}}
\renewcommand*{\cftsecleader}{\cftdotfill{\cftdotsep}}
\renewcommand*{\cftsubsecleader}{\cftdotfill{\cftdotsep}} 
\renewcommand*{\cftfigleader}{\cftdotfill{\cftdotsep}} 
\renewcommand*{\cfttableader}{\cftdotfill{\cftdotsep}} 



\newcommand*{\acknowledgmentsname}{\th@ChapHead{\uppercase{Acknowledgments}}}

\newcommand*{\dedicationname}{\th@ChapHead{\uppercase{Dedication}}}

\newcommand*{\abbreviationname}{\th@ChapHead{\uppercase{List of Abbreviations}}}

\newcommand*{\prefacename}{\th@ChapHead{\uppercase{Preface}}}



\renewcommand*{\contentsname}{\th@ChapHead{\vspace*{0.50in}\uppercase{Table of Contents}\vspace*{-1.00in}}}

\renewcommand*{\listfigurename}{\th@ChapHead{\vspace*{0.5in}\uppercase{List of Figures}\vspace*{-0.50in}}}

\renewcommand*{\listtablename}{\th@ChapHead{\vspace*{0.5in}\uppercase{List of Tables}\vspace*{-0.50in}}}



\renewcommand*{\bibname}{\th@ChapHead{\vspace*{0.67in}BIBLIOGRAPHY\vspace*{-.80in}}}

\renewcommand*{\chaptername}{\th@ChapHead{\uppercase{Chapter}}}

\renewcommand*{\appendixname}{\th@ChapHead{\uppercase{Appendix }}}


\newcommand*{\chapternameToC}{\normalsize\th@HdFnt \normalfont} 

\newcommand*{\bibnameToC}{\normalsize\th@HdFnt\vspace*{-0.01in}\uppercase{Bibliography}\normalfont}

\newcommand*{\appendixnameToC}{\normalsize\th@HdFnt\uppercase{Appendix}\normalfont}




% Adds the appropriate label to the ToC/LoF/LoT

\addtocontents{lof}{\protect\contentsline {part}
    {\protect\figurenameToC}{\protect\relax}{}}

\addtocontents{lot}{\protect\contentsline {part}
    {\protect\tablenameToC}{\protect\relax}{}}

\addtocontents{toc}{\protect\contentsline {part}
    {\protect\chapternameToC}{\protect\relax}{}}




\addtocontents{toc}{\protect\contentsline {part}
    {\protect\chapternameToC}{\protect\relax}{}}



% NUMBERING STYLES, DEFAULT VALUES

% Using macros of the form \th@Def* avoids conflicts with
% LaTeX internals which use \theDef* forms...

\newcounter{th@SecNum}
\setcounter{th@SecNum}{3}
\newcommand*{\th@DefTheChapter}{\arabic{chapter}}
\newcommand*{\th@DefTheSection}{\th@DefTheChapter.\arabic{section}}
\newcommand*{\th@DefTheSubsection}{\th@DefTheSection.\arabic{subsection}}
\newcommand*{\th@DefTheSubsubsection}{\th@DefTheSubsection.\arabic{subsubsection}}


\renewcommand*{\thechapter}{\th@DefTheChapter}
\renewcommand*{\thesection}{\th@DefTheSection}
\renewcommand*{\thesubsection}{\th@DefTheSubsection}
\renewcommand*{\thesubsubsection}{\th@DefTheSubsubsection}



% Enumerated List labels
\renewcommand*{\theenumi}{\arabic{enumi}}
\renewcommand*{\theenumii}{\alph{enumii}}
\renewcommand*{\theenumiii}{\roman{enumiii}}
\renewcommand*{\theenumiv}{\alph{enumiv}}
\renewcommand*{\labelenumi}{(\theenumi)}
\renewcommand*{\labelenumii}{(\theenumii)}
\renewcommand*{\labelenumiii}{(\theenumiii)}
\renewcommand*{\labelenumiv}{(\theenumiii.\theenumiv)}

% Itemized list labels.
\renewcommand*{\labelitemi}{\textbullet}
\renewcommand*{\labelitemii}{\textasteriskcentered}
\renewcommand*{\labelitemiii}{\normalfont \bfseries \textendash}
\renewcommand*{\labelitemiv}{\textperiodcentered}

% Margins for lists
\setlength{\leftmargini}{3.5em}
\setlength{\leftmarginiii}{2.2em}
\setlength{\leftmarginiv}{2.2em}
\setlength{\leftmarginv}{2.2em}
\setlength{\leftmarginvi}{2.2em}
\setlength{\leftmargin}{\leftmargini}
\setlength{\labelwidth}{1.5em}

% Redefine certain commands and environments from report.cls.
% This is where things get messy.  We rename each command we modify,
% then redefine the command using the old version as subset.

% VERSE, QUOTATION, and QUOTE must be single-spaced.
% These rename the internal commands for the \begin{...} and
% \end{...} environment commands.
\let\th@OldVerse\verse
\let\th@OldQuotation\quotation
\let\th@OldQuote\quote
\let\endth@OldVerse\endverse
\let\endth@OldQuotation\endquotation
\let\endth@OldQuote\endquote

% Note - the ``\begin{singlespace}'' and ``\end{singlespace}''
% are defined in setspace.sty.

\renewenvironment*{verse}{\begin{th@OldVerse}\begin{singlespace}}
    {\end{singlespace}\end{th@OldVerse}}

\renewenvironment*{quotation}{\begin{th@OldQuotation} \begin{singlespace}}
    {\end{singlespace}\end{th@OldQuotation}}

\renewenvironment*{quote}{\begin{th@OldQuote} \begin{singlespace}}
    {\end{singlespace} \end{th@OldQuote}}

% Theorems and Axioms:  They must NOT be italicized.  To do this
% robustly, I've needed to use some in-depth TeXnology.  Check out a
% good LaTeX book for info on the \@namedef and \@nameuse commands.

\let\th@OldNewtheorem\newtheorem
% Check for the calling format: \newtheorem{}[]{}
\def\newtheorem#1{ \@ifnextchar[{\th@nuthrmOM{#1}}{\th@newtheoremTwo{#1}}%]
}
% Calling format: \newtheorem{}{}[]; checks for the optional arg at the end.
\def\th@newtheoremTwo#1#2{
    \@ifnextchar[{\th@nuthrmO{#1}{#2}}{\th@nuthrm{#1}{#2}}%]
}
% Standard call.  No optional args.
\def\th@nuthrm#1#2{
    \th@OldNewtheorem{th@#1}{#2}
    \th@correctTheorem{end#1}{#1}{endth@#1}{th@#1}
}
% Optional arg at end.
\def\th@nuthrmO#1#2[#3]{
    \th@OldNewtheorem{th@#1}{#2}[#3]
    \th@correctTheorem{end#1}{#1}{endth@#1}{th@#1}
    }
% Optional arg in the middle.
\def\th@nuthrmOM#1[#2]#3{
    \th@OldNewtheorem{th@#1}[th@#2]{#3}
    \th@correctTheorem{end#1}{#1}{endth@#1}{th@#1}
    }
\def\th@execThrmOpt#1[#2]{
    \@nameuse{#1}[#2] \normalsize\normalfont
}
\newcommand*{\th@correctTheorem}[4]{
    \@namedef{#1}{\@nameuse{#3}}
    \@namedef{#2}{\@ifnextchar[{\th@execThrmOpt{#4}}{ %%}
            \@nameuse{#4} \normalsize\normalfont}
    }
}

% APPENDIX command
%\newcounter{th@SectionSave}
%\let\th@OldAppCmd\appendix
%\renewcommand{\appendix}{
%   \setcounter{th@SectionSave}{\value{section}}
%    \th@OldAppCmd
%    \setcounter{section}{0}%
%    \setcounter{subsection}{0}% 
%    \addtocontents{toc}{\protect\contentsline {part}
%        {\protect\hspace*{-0.0625in}\appendixnameToC}{\protect\relax}{}}
%    \renewcommand*{\th@DefTheChapter}{\Alph{chapter}}
%  }

% BIBLIOGRAPHY: Used to be redefined here.  Moved to permit use
% of different bibliography .sty and .bst files.  It's now in
% the '\AtBeginDocument' command.

% SECTION HEADINGS.
% including several new options

% New if-s
\newif\if@AlwaysFalse
\@AlwaysFalsefalse
\newif\if@CenterHeading
\@CenterHeadingfalse
\newif\if@UseStarCmd
\@UseStarCmdfalse




% GENERAL AND SETUP COMMANDS.

% Makes a heading.
% The optional argument (#1) is a centering flag. The heading gets
% centered if it's present and set to true.
% The second arg is the style command.
% The third arg is the heading proper.
\newcommand*{\th@MakeHeading}[2]{
    \protect #1 \hspace{-0.1in}
    \if@Section\centering #2\else #2 \fi
    \vspace{-0.15in}
    }

% Makes a heading.
% The entire thing is something of a mess.  We want all ToC entries to
% be in normal text.  We also want references in normal text.  But, we
% want the heading numbers to appear in the correct size and font.  To
% get this to happen is a chore.

% The args are, in order:
%    The command to execute (#1),
%    The heading style command (#2),
%    The optional arg passed to the heading command proper (#3).
%    E.g. if you execute a \section[foo]{bigfoo}, #3=="foo".
%    The heading text itself (#4).
%    The name of the current command (#5). E.g. "chapter".
%    The internal command that generates the number label (#6).

% There are also two important flags used by this command.  The first
% is \@UseStarCmd.  Set this to "true" if this is the starred version
% of the command.  The second flag is \@thInHeading.  This gets set
% 'true' inside the section heading commands and turned off at the end
% of the command.

\newcommand*{\th@Heading}[6]{
        \vspace{-0.1in}  
    \@thInHeadingtrue
    % Alter the '\the...' command.
    \@namedef{the#5}{\protect#2 #6}
    \if@UseStarCmd
        #1*{\th@MakeHeading{\protect#2}{#4}}
    \else
        \ifthenelse{\equal{\th@DefArg}{#3}}{
            #1[\normalsize\normalfont#4]{
                \th@MakeHeading{\protect#2}{#4}}
        }{
            #1[\normalsize\normalfont#3]{
                \th@MakeHeading{\protect#2}{#4}}
        }
    \fi
% Reset the '\the...' command, but don't enforce
% '\normalsize\normalfont' anywhere, as this messes up
% cross-references in footnotes.
    \@namedef{the#5}{#6}
    \@thInHeadingfalse
    \@CenterHeadingfalse
    \normalsize\normalfont
}

% CHAPTER HEADING

% The chapter command definitely needs some re-defining.  The primary
% purpose is to control numbering in figures, tables, and equations.

% Special if-s; used internally for chapters.
\newif\if@THshortChapter
\@THshortChapterfalse

% SECTION HEADING COMMANDS.  We need to use the starred version and
% add the numbering and toc-entry ourselves.  We need to do this for
% all of the subsequent heading commands.  If we don't we'll get
% weird-looking ToC entries and references.

\let\th@OldSection\section
\renewcommand*{\section}{\@ifstar{\th@SectionS}{\th@Section}}

\newcommand*{\th@Section}[2][\th@DefArg]{
    \@Sectiontrue
    \@UseStarCmdfalse 
    \@CenterHeadingtrue\@Sectiontrue
    \th@Heading{\th@OldSection}{\th@SectionStyle}{#1}
    {{\it #2}}{section}{\th@DefTheSection} 
    \normalsize\normalfont
}


\newcommand*{\th@SectionS}[1]{
    \@UseStarCmdtrue
    \@CenterHeadingtrue
    \th@Heading{\th@OldSection}{\th@SectionStyle}{\th@DefArg}
        {#1}{section}{\th@DefTheSection}
    \@UseStarCmdfalse
    \normalsize\normalfont
}







% SUBSECTION HEADING COMMANDS.

\let\th@OldSubSection\subsection
\renewcommand*{\subsection}{\@ifstar{\th@SubSectionS}{\th@SubSection}
}

\newcommand*{\th@SubSection}[2][\th@DefArg]{
     \@Sectionfalse
    \@UseStarCmdfalse
    \@CenterHeadingfalse
    \th@Heading{\th@OldSubSection}{\th@SubSectionStyle}
        {#1}{{\it #2}}{subsection}{\th@DefTheSubsection}
    \normalsize\normalfont
}

\newcommand*{\th@SubSectionS}[1]{
    \@UseStarCmdtrue
    \@CenterHeadingfalse
    \th@Heading{\th@OldSubSection}{\th@SubSectionStyle}
        {\th@DefArg}{#1}{subsection}{\th@DefTheSubsection}
    \@UseStarCmdfalse
    \normalsize\normalfont
}

% SUBSUBSECTION HEADING COMMANDS.  No starred version permitted.
% No ToC entry.  We also need to check and see if we should number
% it or not and if we should inline it or not.

\let\th@OldSubSubSection\subsubsection
\newlength{\th@SubSubSecSpc}

\renewcommand*{\thesubsubsection}{\th@DefTheSubsubsection}
\newcommand*{\th@PreSubsubsecCmd}{\@UseStarCmdfalse}
\setcounter{th@SecNum}{4}
\setlength{\th@SubSubSecSpc}{0.5em}

\renewcommand*{\subsubsection}[2][\th@DefArg]{
    \refstepcounter{subsubsection}
    \vspace{4\parsep}
    \@thInHeadingtrue\th@OtherSecHeadStyles%

\ifsecnum
        \thesubsubsection \hspace{\th@SubSubSecSpc}{\it #2.}\hspace{2mm}
\else
        \hspace{\th@SubSubSecSpc}{\it #2.}\hspace{2mm}
\fi      
    \ignorespaces\normalsize\normalfont
    }

%%
%% Style guidelines don't seem to permit named paragraphs and
%% subparagraphs, so we'll disable them.
%%
\renewcommand*{\paragraph}[2][\th@DefArg]{
    \ClassError{thesis}{Named paragraphs are not allowed in a thesis}{}
}
\renewcommand*{\subparagraph}[2][\th@DefArg]{
    \ClassError{thesis}{Named subparagraphs are not allowed in a thesis}{}
}

% NEW COMMANDS DEFINED FOR THIS CLASS

% Unavailable commands
\renewcommand*{\theindex}{\ClassWarning{thesis}
   {Indexing not available in this class.}}
\renewcommand*{\twocolumn}{\ClassWarning{thesis}
    {Twocolumn not available in this class.}}
\renewcommand*{\thepart}{\ClassWarning{thesis}
    {``thepart'' not available in this class.}}
\renewcommand*{\part}{\ClassWarning{thesis}
    {``thepart'' not available in this class.}}


% NEW COMMANDS.
% This forces a page number on the first page of a chapter, etc.
\newcommand*{\forcepageno}{\thispagestyle{myheadings}}
\newcommand*{\OnePageChapter}{\forcepageno}

% THESIS PROLOGUE PAGES.
% Stores the second arg in the command specified by the first arg.
\newcommand{\th@StoreIn}[2]{ \gdef#1{#2} }

% Default values of storage variables.  Emits an error message.
% This part of the class is long...

\newcommand*{\thesisAuthorName}{
    \ClassError{thesis}{You must give your name using \protect\author{}}
}
\newcommand*{\thesisTitle}{
    \ClassError{thesis}{You must give a title using \protect\title{}}
}
\newcommand*{\thesisReader}{
    \ClassError{thesis}{You must give a reader using \protect\reader{}}
}

%%
%% Give default values for optional parameters.
%%
\newcommand*{\th@readerThree}{none}
\newcommand*{\th@readerFour}{none}
\newcommand*{\th@readerFive}{none}
\newcommand*{\th@readerSix}{none}

\newcommand{\th@abstractStash}{
    \ClassError{thesis}{
        You must specify an abstract!}
    {Use the \protect\abstractpage[]{} command in the \MessageBreak
        preamble.  If your abstract runs over a page, \MessageBreak
        use the optional argument with the word 'long'.}
}
%by Daniel

\newcommand*{\th@signature}[1]{ 
        \begin{center}
        \normalsize
        \vspace*{2.5ex}
        \vrule width 80mm height 0.2mm
        \\
        #1 
        \end{center}
}

\newcommand*{\th@dateline}{
    \vspace*{9mm}
    \begin{flushright}
        Date~{\vrule width 35mm height 0.2mm}
    \end{flushright}
}
\newcommand{\th@AbstractPageStyle}{\relax}
\newcommand{\th@dedicationStash}{\relax}
\newcommand{\th@dedicationTitleStash}{\relax}
\newcommand{\th@abbreviationStash}{\relax}
\newcommand{\th@abbreviationTitleStash}{\relax}
\newcommand{\th@prefaceStash}{\relax}
\newcommand{\th@prefaceTitleStash}{\relax}
\newcommand{\th@acknowledgementsStash}{\relax}
\newif\if@shortLoF
\newif\if@shortLoT
\newif\if@shortToC
\newif\if@makenoLoF
\newif\if@makenoLoT
\newif\if@makeDedication
\newif\if@makeAbbreviation
\newif\if@makePreface
\newif\if@makeAcknowledgements
\newif\if@makePrologue
\newif\if@makeThesisCopyrightPage
\@shortLoFfalse
\@shortLoTfalse
\@shortToCfalse
\@makenoLoFfalse
\@makenoLoTfalse
\@makeThesisCopyrightPagefalse
\@makeDedicationfalse
\@makePrefacefalse
\@makeAbbreviationfalse
\@makeAcknowledgementsfalse
\@makeProloguetrue

% The base commands that make all of the prologue pages.

\newcommand{\th@Abstractpage}{
        \enlargethispage{-.75in}
   \pagestyle{empty}
        \doublespacing
        \vspace*{1.08in} % modified by John was .995
    \begin{center}
                ABSTRACT
                \ \\ 
        \singlespacing
        \thesisTitle
                \ \\
         \doublespacing
                \thesisAuthorName, Ph.D.
                \ \\
       Chairperson: \thesisMentor
    %\end{list}
        \vspace*{0.07in}
        \end{center}
        %\doublespacing
    \th@abstract    % finally, the actual abstract         

}

%\newcommand*{\thesisTitlePage}{
%        \thispagestyle{empty} 
%    \doublespacing
%        \pagenumbering{roman}
%        \vspace*{1.055in} %modified by John
%    \begin{center} \thesisTitle \end{center}
%        \vspace*{1.33in}%modified by Daniel
%    \begin{center} A Dissertation Submitted  to the Graduate Faculty of\\ 
%    Baylor University\\ 
%        in Partial Fulfillment of the\\
%        Requirements for the Degree\\
%        of\\
%    Doctor of Philosophy\\
%        \vspace*{0.75in}%modified by Daniel
%    By\\
%        \thesisAuthorName\\
%        \vspace{1.35in}%modified by Daniel
%    Waco, Texas\\
%    \thesisConfDate
%        \end{center}
%        \pagebreak
%}



%%
%% Make the signature page modified by John 
\newcommand*{\thesisMakeSignaturePage}{
%    \enlargethispage{1in}% creates .75" from bottom of dean name to page bottom
%                         % and 15/16" = .9375" from bottom of sig line to page bottom
    \enlargethispage{.75in}% creates .75" from bottom of dean name to page bottom
                         % and 15/16" = .9375" from bottom of sig line to page bottom
        \thispagestyle{empty}   
    \singlespacing
    \begin{center}
\doublespacing
        \singlespacing
        \thesisTitle
        \ \\
       \doublespacing by
        \ \\
       \thesisAuthorName, \yourDegrees
      %\thesisAuthorName, B.S., M.S.
        \ \\
        A Dissertation
        \ \\     
        Approved by the Department of Educational Psychology

 \singlespacing
 
    \th@signature{\thesisDeptChair , Chairperson}

        Submitted to the Graduate Faculty of\\
        Baylor University in Partial Fulfillment of the\\
        Requirements for the Degree\\
        of\\
        Doctor of Philosophy
    \end{center}
        \vspace*{0.40in}
    \begin{flushleft}
\hspace*{0.4in} 
\begin{tabular*}{80mm}[t]{c}
        Approved by the Dissertation Committee  \\
        \\
\\ \hline
    \thesisMentor , Chairperson \\
    \\
\\ \hline
    \thesisReader  \\ % REQUIRED.
    \\
\\ \hline
    \th@readerThree \\ % optional 
    \\
\\ \hline
    \th@readerFour \\ % optional 
    \\
\\ \hline
    \th@readerFive \\ % optional 
        \\
%\\ \hline
   % \th@readerSix \\ % optional 
\end{tabular*}
    \end{flushleft}
    %\vspace*{.0000000025in}
    \vfill
    
    \begin{flushright}
\begin{tabular*}{65mm}{c}
        Accepted by the Graduate School \\
        \thesisConfDate \\
\\
\\ \hline
        \thesisGraduateDean , Dean 
\end{tabular*}
    \end{flushright}
    \vspace*{.10in}
    \begin{center}
%\begin{tabular*}{50mm}[b]{c}
    \scriptsize{\it {Page bearing signatures is kept on file in the Graduate School.}}
%\end{tabular*}
\end{center}
        \thispagestyle{empty}      
        \pagebreak
}




% Make a copyright page for this thesis.

\newcommand*{\thesisCopyrightPage}{
        \thispagestyle{empty}
    \if@makeThesisCopyrightPage
      \vspace*{8.05in} %modified by John
      %next line modified by Daniel
      \begin{center} 
      Copyright \copyright \ \the\year\ by \thesisAuthorName \\
          \vspace{0.1in}
      All rights reserved
      \end{center}
        \else
          % Force a blank page
          \vspace*{0.1in}
          \relax
        \fi
    \pagebreak
}


%
% Make the signature page
% modified by Daniel
%\newcommand*{\thesisMakeSignaturePage}{
   %     \thispagestyle{empty}   
    %\singlespacing
    %    \vspace*{1.20625in}%modified by John
    %\begin{center}
     %   Approved by the Department of Mathematics:
 
    %\th@signature{\thesisDeptChair , Chairperson}

     %   \vspace*{1.7in}
      %  Approved by the Dissertation Committee:

    %\th@signature{\thesisMentor , Chairperson}

    %\th@signature{\thesisReader}% REQUIRED.

    %\ifthenelse{\equal{\th@readerThree}{none}}{\relax}
     %           {\th@signature{\th@readerThree}} % optional

    %\ifthenelse{\equal{\th@readerFour}{none}}{\relax}
     %           {\th@signature{\th@readerFour}} % optional

   % \ifthenelse{\equal{\th@readerFive}{none}}{\relax}
               % {\th@signature{\th@readerFive}} % optional    
       % \vspace*{1.3in}%modified by John
        %Approved by the Graduate School:

        %\th@signature{\thesisGraduateDean , Dean}
    %\end{center}
     %   \thispagestyle{empty}      
      %  \pagebreak
%}




\newcommand{\th@Acknowledgementpage}{
     \thispagestyle{plain}   
    \if@makeAcknowledgements
                %modified by Alex
    \addcontentsline{toc}{chapter}{\normalsize\hspace*{-0.0625in}\acknowledgmentsname\normalfont}
        % \addcontentsline{toc}{chapter}{\normalfont\normalsize  \vskip -0.35in ACKNOWLEDGMENTS\vspace*{-.35in}}
        \vspace*{0.20in}
        \normalfont\normalsize \begin{center} ACKNOWLEDGMENTS
\end{center} \vspace{4\parsep}
        \doublespacing
        \th@acknowledgementsStash
    \fi
}

\newcommand{\th@Dedicationpage}{
    \clearpage
        \thispagestyle{plain}
        \if@makeDedication
                %modified by Alex
\addcontentsline{toc}{chapter}{\normalsize\hspace*{-0.0625in} \dedicationname\normalfont}
        	% \addcontentsline{toc}{chapter}{\normalfont\normalsize\vskip.17in DEDICATION\vspace*{0in}}
        \vspace*{0.06in}
        \normalfont\normalsize \begin{center} DEDICATION
\end{center} \vspace{0.8in}
        \doublespacing
        \begin{center}
       \th@dedicationStash
       \end{center}
         \ifthenelse{\equal{\th@DefArg}{\th@dedicationTitleStash}
            }{\vfill}{\relax}
    \fi
}



\newcommand{\th@Abbreviationpage}{
    \clearpage
        \thispagestyle{plain}
        \if@makeAbbreviation
                %modified by Alex
\addcontentsline{toc}{chapter}{\normalsize\hspace*{-0.0625in} \abbreviationname\normalfont}
        \vspace*{0.06in}
        \normalfont\normalsize \begin{center} \uppercase{List of Abbreviations}
\end{center} \vspace{5\parsep}
        \doublespacing
        \begin{center}
       \th@abbreviationStash
       \end{center}
         \ifthenelse{\equal{\th@DefArg}{\th@abbreviationTitleStash}
            }{\vfill}{\relax}
    \fi
    \pagebreak
}


\newcommand{\th@Prefacepage}{
    \clearpage
        \thispagestyle{plain}
        \if@makePreface
\addcontentsline{toc}{chapter}{\normalsize\hspace*{-0.0625in} \prefacename\normalfont}
        \vspace*{0.06in}
        \normalfont\normalsize \begin{center} \uppercase{Preface}
\end{center} \vspace{4\parsep}
        \doublespacing
        \begin{center}
       \th@prefaceStash
       \end{center}
         \ifthenelse{\equal{\th@DefArg}{\th@prefaceTitleStash}
            }{\vfill}{\relax}
    \fi
    \pagebreak
}


\newcommand{\th@ToCpage}{
    \@THshortChapterfalse
    \if@shortToC
        \@THshortChaptertrue
        \typeout{Making Short ToC...}
    \fi
    \@thInHeadingfalse
    {\singlespacing
 %only produce chapter and section in TOC
\setcounter{tocdepth}{1}
    \setlength{\cftbeforechapskip}{10pt}
    \setlength{\cftbeforesecskip}{0pt}
\renewcommand\cftchapafterpnum{\vskip0in}
\renewcommand\cftsecafterpnum{\vskip0in}
% \setlength{\cftbeforesubsecskip}{0pt}
% \setlength{\cftbeforesubsubsecskip}{0pt}
\renewcommand{\cftchappagefont}{}
\renewcommand{\cftpartpagefont}{}
 \renewcommand{\@tocrmarg}{2.9em plus1fil}
% dot spacing in TOC
 \renewcommand{\cftdotsep}{1}
% indent section
\setlength{\cftsecindent}{\dimexpr\cftchapnumwidth+1.5em}
% italcize section
\renewcommand{\cftsecfont}{\itshape}
% Chapter number and name on two different lines
\setlength{\cftchapnumwidth}{15pt}
\renewcommand{\cftchappresnum}{\chaptername}
\renewcommand{\cftchapaftersnumb}{\newline}
% Switch to Appendix in TOC instead of Chapter
\g@addto@macro\appendix{%
  \addtocontents{toc}{%
    \protect\renewcommand{\protect\cftchappresnum}{\appendixname\space}%
  }%
}
\tableofcontents
        }
}




\newcommand{\th@LoFpage}{
         \singlespacing
% put the word Figure in name
\renewcommand{\cftfigpresnum}{Figure\space}
% period after figure number
\renewcommand{\cftfigaftersnum}{.}
    \addcontentsline{toc}{chapter}{\normalsize\hspace*{-0.0625in}
\listfigurename\normalfont}
% dot spacing 
 \renewcommand{\cftdotsep}{1}
 \setlength{\cftbeforefigskip}{10pt}
 \setlength{\cftfigindent}{0.0in}
\newlength{\mylenf}
\settowidth{\mylenf}{\cftfigpresnum}
\setlength{\cftfignumwidth}{\dimexpr\mylenf+2.5em}
 \renewcommand{\@tocrmarg}{2.9em plus1fil}
        \renewcommand*{\addvspace}[1]{}% no extra space between chapters
    \listoffigures   
}



\newcommand{\th@LoTpage}{
    \typeout{Making Short LoT...}
    \singlespacing
% put the word table in name
\renewcommand{\cfttabpresnum}{Table\space}
% period after table number
\renewcommand{\cfttabaftersnum}{.}
     \addcontentsline{toc}{chapter}{\normalsize\hspace*{-0.0625in}
\listtablename\normalfont}
% dot spacing 
 \renewcommand{\cftdotsep}{1}
 \setlength{\cftbeforetabskip}{10pt}
  \setlength{\cfttabindent}{0.0in}
\newlength{\mylent}
\settowidth{\mylent}{\cfttabpresnum}
\setlength{\cfttabnumwidth}{\dimexpr\mylent+2.5em}
   \renewcommand{\@tocrmarg}{2.9em plus1fil}
        \renewcommand*{\addvspace}[1]{}
    \listoftables
}



% This performs the actual generation of the prologue

\newcounter{th@storepage}
\newcommand{\th@MakeThesisPrologue}{
\if@makePrologue
        \pagenumbering{roman}
        \th@Abstractpage
                \pagebreak
        \thesisMakeSignaturePage
       % \thesisTitlePage
        \thesisCopyrightPage
         % \addtocounter{page}{-1}
         \setcounter{th@storepage}{\value{page}}
                \pagestyle{plain}    
        \th@ToCpage 
        \pagebreak
                \pagestyle{plain} 
        \if@makenoLoF
          \relax
        \else
         \th@LoFpage
                \pagebreak
                \pagestyle{plain}
                \fi 
        \if@makenoLoT
          \relax
        \else
          \th@LoTpage
                  \pagebreak
          \fi
        \th@Abbreviationpage
		\th@Prefacepage
        \th@Acknowledgementpage 
        \th@Dedicationpage
       
    \fi
    
    \pagestyle{plain}
}

%%
%% Run some special commands when the document first starts.
%%
\let\th@OldBeginDocument\document
\renewcommand*{\document}{
    \th@OldBeginDocument
    \th@MakeThesisPrologue
    \doublespacing
    \clearpage
    \pagenumbering{arabic}
    \@THshortChapterfalse
}

%%
%% Parameters expected in the prologue:
%%

\newcommand*{\sectionnumbering}[1]{\th@StoreIn{\secnumcall}{#1}}


\renewcommand*{\title}[1]{\th@StoreIn{\thesisTitle}{#1}}
\newcommand*{\mentor}[1]{\th@StoreIn{\thesisMentor}{#1}}
\newcommand*{\reader}[1]{\th@StoreIn{\thesisReader}{#1}}
\newcommand*{\deptChair}[1]{\th@StoreIn{\thesisDeptChair}{#1}}
\newcommand*{\graduateDean}[1]{\th@StoreIn{\thesisGraduateDean}{#1}}
\newcommand*{\confDate}[1]{\th@StoreIn{\thesisConfDate}{#1}}

\newcommand*{\readerThree}[1]{\th@StoreIn{\th@readerThree}{#1}}
\newcommand*{\readerFour}[1]{\th@StoreIn{\th@readerFour}{#1}}
\newcommand*{\readerFive}[1]{\th@StoreIn{\th@readerFive}{#1}}
\newcommand*{\readerSix}[1]{\th@StoreIn{\th@readerSix}{#1}}

\newcommand{\acknowledgements}[1]{
    \@makeAcknowledgementstrue
    \th@StoreIn{\th@acknowledgementsStash}{\hspace*{0.42in}#1}
}

\newcommand{\dedication}[2][\th@DefArg]{
    \@makeDedicationtrue
    \th@StoreIn{\th@dedicationTitleStash}{#1}
    \th@StoreIn{\th@dedicationStash}{#2}
}

\newcommand{\abbreviation}[2][\th@DefArg]{
    \@makeAbbreviationtrue
    \th@StoreIn{\th@abbreviationTitleStash}{#1}
    \th@StoreIn{\th@abbreviationStash}{#2}
}

\newcommand{\preface}[2][\th@DefArg]{
    \@makePrefacetrue
    \th@StoreIn{\th@prefaceTitleStash}{#1}
    \th@StoreIn{\th@prefaceStash}{#2}
}



\renewcommand*{\author}[1]{
    \th@StoreIn{\thesisAuthorName}{#1}
}

\newcommand*{\degrees}[1]{
    \th@StoreIn{\yourDegrees}{#1}
}

\renewcommand{\abstract}[1]{
    \th@StoreIn{\th@abstract}{\hspace*{0.42in}#1}
}

\newcommand*{\SuspendPrologue}{
    \@makeProloguefalse
    \typeout{No prologue pages printed.}
}

\newcommand*{\makeCopyrightPage}{
    \@makeThesisCopyrightPagetrue
    \typeout{Copyright page will be printed.}
}

\newcommand*{\emptyLoF}{
    \@makenoLoFtrue
    \typeout{No LoF printed}
}

\newcommand*{\emptyLoT}{
    \@makenoLoTtrue
    \typeout{No LoT printed}
}

\newcommand*{\ToCisShort}{
    \@shortToCtrue
    \typeout{Putting page number on short ToC}
}

\newcommand*{\LoFisShort}{
    \@shortLoFtrue
    \typeout{Putting page number on short LoF}
}

\newcommand*{\LoTisShort}{
    \@shortLoTtrue
    \typeout{Putting page number on short LoT}
}

%From here on, is all modified by Daniel

\renewcommand{\section}{\@startsection {section}{1}{0pt}%
         {4ex plus 0ex minus 0ex}%
         {0.001ex plus 0ex minus 0ex}%
         {\center\normalfont\itshape\normalsize}}



\renewcommand{\subsection}{\@startsection {subsection}{2}{0pt}%
         {4ex plus 0ex minus 0ex}%
         {0.001ex plus 0ex minus 0ex}%
         {\normalfont\itshape\normalsize}}

% ===========================================================
% = HAck to get triple space before section and subsections =
% ===========================================================

\let\origsection\section
\renewcommand\section{\@ifstar{\starsection}{\nostarsection}}

\newcommand\nostarsection[1]
{\sectionprelude\origsection{#1}}

\newcommand\starsection[1]
{\sectionprelude\origsection*{#1}}

\newcommand\sectionprelude{%
  \vspace{3\parsep}
}


\let\origsubsection\subsection
\renewcommand\subsection{\@ifstar{\starsubsection}{\nostarsubsection}}

\newcommand\nostarsubsection[1]
{\subsectionprelude\origsubsection{#1}}

\newcommand\starsubsection[1]
{\subsectionprelude\origsubsection*{#1}}

\newcommand\subsectionprelude{%
  \vspace{3\parsep}
}



%redefinition of the caption,
%it should not have a colon after the figure number, 
%instead it should have a point
\long\def\@makecaption#1#2{%
  \vskip\abovecaptionskip
  \sbox\@tempboxa{#1. #2}%
  \ifdim \wd\@tempboxa >\hsize
    #1: #2\par
  \else
    \global \@minipagefalse
    \hb@xt@\hsize{\hfil\box\@tempboxa\hfil}%
  \fi
\vskip\belowcaptionskip}






%% Moved from BaylorThesisExample.tex

% ==================
% = Misc. Packages =
% ==================
% Put your specific packages here.

% you do NOT need this package. It generates filler text
\usepackage{lipsum}

%Math typesetting packages
\usepackage{amsfonts, amssymb, amsmath, latexsym, amsthm}

%for URLs in-text 
\usepackage{url}

% ==============
% = Formatting =
% ==============

\voffset=-.6875in
\setlength{\textheight}{9.65in}

% allows accented output characters
\usepackage[T1]{fontenc}
% allows user to input accented characters directly from the keyboard
\usepackage[utf8]{inputenc}


% A serif font is required (e.g. Times New Roman). The defulat font is Computer Modern Roman. 
% Other fonts can be found at http://www.tug.dk/FontCatalogue/seriffonts.html

% minimize widows and orphans
\usepackage[all]{nowidow}



% justification--ragged right
\usepackage{ragged2e}
\setlength{\RaggedRightParindent}{6mm}

% single spacing after periods
\frenchspacing


% ================
% = Bibliography =
% ================
%APA style citations and references
\usepackage[natbibapa]{apacite} 
% for hanging-indentation style using apacite
\setlength{\bibindent}{2.5em}
\setlength{\bibleftmargin}{0em}



% ==========
% = Floats =
% ==========
\usepackage{float}

% include external pictures
\usepackage{graphicx} %Graphics/figures

% rotate figures/tables
\usepackage{rotating} 

% For professional tables
\usepackage{booktabs,threeparttable} 

% To draw path models
\usepackage{tikz}
\usetikzlibrary{calc,arrows,positioning,shapes,patterns}

% multipage tables
\usepackage{longtable} 
\setlength\LTleft{0pt}
\setlength\LTright{0pt}



% To have tables that allows Returns
\usepackage{array}
\newcolumntype{C}[1]{>{\centering\let\newline\\\arraybackslash\hspace{0pt}}p{#1}}
\newcolumntype{L}[1]{>{\raggedright\let\newline\\\arraybackslash\hspace{0pt}}p{#1}}

\usepackage{placeins} % mainly for \FloatBarrier



\usepackage{graphicx} % new way of doing eps files
\usepackage{listings} % nice code layout
\usepackage{amsmath}
\usepackage{placeins} % mainly for \FloatBarrier
\usepackage{mathtools} % for rcases*, dcases etc
\usepackage[normalem]{ulem} % for underlining text, e.g. \uline{...}, \uwave{}, \sout{}, \xout{}, etc.
\usepackage{marginnote} % for margin notes
\usepackage{amssymb}    % for \mathbb
\usepackage{mathrsfs}   % for fonts \mathcal{}, \mathscr{}, etc
\usepackage[makeroom]{cancel} % used for generate "cross out" effect
\usepackage{hyperref}
\hypersetup{
    colorlinks=true,
    linkcolor=blue,
    filecolor=magenta,
    urlcolor=cyan,
}
% \usepackage{hypcap}
\usepackage{enumerate} % enumerate with different letter style
\usepackage{enumitem}  % \begin{enumerate}[label=\textbf{\alph*}]
\usepackage{cases} % numcases and subnumcases

\usepackage{blindtext}
\usepackage{scrextend} % for labeling command

\usepackage{pifont}  % checkmark commands: \checkmark
\newcommand{\cmark}{\ding{51}}%
\newcommand{\xmark}{\ding{55}}%

\usepackage{accents} % fix buggy \dddot{}
\usepackage[]{algorithm2e}
% Usage:
% \begin{algorithm}[H]
% \end{algorithm}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Custom Commands
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

\newcommand*\rfrac[2]{{}^{#1}\!/_{#2}}
\newcommand{\eqnote}[1]{\mbox{\hspace{12pt}#1\hspace{12pt}}}
% \newcommand{\eqtext}[1]{\mbox{\hspace{12pt}#1\hspace{12pt}}}
\newcommand{\eqtextbegin}[1]{\mbox{#1\hspace{12pt}}}
\newcommand{\eqtextend}[1]{\mbox{\hspace{12pt}#1\hspace{3pt}}}

\newcommand\numberthis{\addtocounter{equation}{1}\tag{\theequation}} % add equation number in align*

\newcommand{\floor}[1]{\left \lfloor #1 \right \rfloor}
\newcommand{\ceil}[1]{\left \lceil #1 \right \rceil}
\newcommand{\abs}[1]{\left| #1 \right|}
\newcommand{\QED}{\hfill\blacksquare}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Shortcuts
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
\newcommand{\p}{\partial}
\newcommand{\df}{\dfrac}
\newcommand{\f}{\frac}
\newcommand{\ol}{\overline}
\newcommand{\OP}{\operatorname}
\newcommand\eqtext[1]{\stackrel{\mathllap{\normalfont\tiny\mbox{#1}}}{=}}
\newcommand{\norm}[1]{\left\lVert#1\right\rVert}

\newtheorem{lemma}{Lemma}
\newtheorem{sublemma}{Lemma}[lemma]
\newtheorem{theorem}{Theorem}
%% ==========================================
%% Indentation:
%% ==========================================

% Make the first line of all sections etc., be indented by the usual paragraph indentation.
% \usepackage{indentfirst}

% used to remove global indent at beginning of paragraph
% parskip will also add a small spacing between paragraphs
% \setlength{\parindent}{0pt}
\usepackage{parskip}
% \usepackage[usenames]{color} % color
\usepackage[]{color} % color
\definecolor{listinggray}{gray}{0.97}
\definecolor{graphgray}{gray}{0.7}
\definecolor{blue}{rgb}{0,0,1}

%% ==========================================
%% Source code listing:
%% ==========================================

\newcommand{\MatLab}[3]{
  \lstset{language=Matlab}
  \lstset{backgroundcolor=\color{listinggray},rulecolor=\color{blue}}
  \lstset{linewidth=\textwidth}
  \lstset{commentstyle=\textit, stringstyle=\upshape,showspaces=false}
  \lstset{frame=tb}
  \lstinputlisting[caption={#1},label={#2}]{#3}
}
% \SciLab{title}{label}{file}
\newcommand{\SciLab}[3]{
  \lstset{language=Scilab}
  \lstset{backgroundcolor=\color{listinggray},rulecolor=\color{blue}}
  \lstset{linewidth=\textwidth}
  \lstset{commentstyle=\textit, stringstyle=\upshape,showspaces=false}
  \lstset{frame=tb}
  \lstinputlisting[caption={#1},label={#2}]{#3}
}
% \Ccode{title}{label}{file}
\newcommand{\Ccode}[3]{
  \lstset{language=c}
  \lstset{backgroundcolor=\color{listinggray},rulecolor=\color{blue}}
  \lstset{linewidth=\textwidth}
  \lstset{commentstyle=\textit, stringstyle=\upshape,showspaces=false}
  \lstset{frame=tb}
  \lstinputlisting[caption={#1},label={#2}]{#3}
}
\newcommand{\Java}[3]{
  \lstset{language=Java}
  \lstset{backgroundcolor=\color{listinggray},rulecolor=\color{blue}}
  \lstset{linewidth=\textwidth}
  \lstset{commentstyle=\textit, stringstyle=\upshape,showspaces=false}
  \lstset{frame=tb}
  \lstinputlisting[caption={#1},label={#2}]{#3}
}
\newcommand{\Bash}[1]{
  \lstset{language=bash}
  \lstset{backgroundcolor=\color{listinggray},rulecolor=\color{blue}}
  \lstset{linewidth=\textwidth}
  \lstset{commentstyle=\textit, stringstyle=\upshape,showspaces=false}
  \lstset{frame=tb}
  #1
}
\newcommand{\BashLang}[3]{
  \lstset{language=bash}
  \lstset{backgroundcolor=\color{listinggray},rulecolor=\color{blue}}
  \lstset{linewidth=\textwidth}
  \lstset{commentstyle=\textit, stringstyle=\upshape,showspaces=false}
  \lstset{frame=tb}
  \lstinputlisting[caption={#1},label={#2}]{#3}
}

\definecolor{orange}{rgb}{1,0.5,0}%Lowercase breaks on \documentclass[jou]{apa}!

% source: http://stackoverflow.com/questions/741985/latex-source-code-listing-like-in-professional-books
% \SourceCode{language}{caption}{label}{file_path}

% Thanks to Alex Ott
% source: http://alexott.net/common/clojure/clj-latex.txt
\lstdefinelanguage{clojure}%
{morekeywords={*,*1,*2,*3,*agent*,*allow-unresolved-vars*,*assert*,*clojure-version*,*command-line-args*,%
*compile-files*,*compile-path*,*e,*err*,*file*,*flush-on-newline*,*in*,*macro-meta*,%
*math-context*,*ns*,*out*,*print-dup*,*print-length*,*print-level*,*print-meta*,*print-readably*,%
*read-eval*,*source-path*,*use-context-classloader*,*warn-on-reflection*,+,-,->,->>,..,/,:else,%
<,<=,=,==,>,>=,@,accessor,aclone,add-classpath,add-watch,agent,agent-errors,aget,alength,alias,%
all-ns,alter,alter-meta!,alter-var-root,amap,ancestors,and,apply,areduce,array-map,aset,%
aset-boolean,aset-byte,aset-char,aset-double,aset-float,aset-int,aset-long,aset-short,assert,%
assoc,assoc!,assoc-in,associative?,atom,await,await-for,await1,bases,bean,bigdec,bigint,binding,%
bit-and,bit-and-not,bit-clear,bit-flip,bit-not,bit-or,bit-set,bit-shift-left,bit-shift-right,%
bit-test,bit-xor,boolean,boolean-array,booleans,bound-fn,bound-fn*,butlast,byte,byte-array,%
bytes,cast,char,char-array,char-escape-string,char-name-string,char?,chars,chunk,chunk-append,%
chunk-buffer,chunk-cons,chunk-first,chunk-next,chunk-rest,chunked-seq?,class,class?,%
clear-agent-errors,clojure-version,coll?,comment,commute,comp,comparator,compare,compare-and-set!,%
compile,complement,concat,cond,condp,conj,conj!,cons,constantly,construct-proxy,contains?,count,%
counted?,create-ns,create-struct,cycle,dec,decimal?,declare,def,definline,defmacro,defmethod,%
defmulti,defn,defn-,defonce,defprotocol,defstruct,deftype,delay,delay?,deliver,deref,derive,%
descendants,destructure,disj,disj!,dissoc,dissoc!,distinct,distinct?,do,do-template,doall,doc,%
dorun,doseq,dosync,dotimes,doto,double,double-array,doubles,drop,drop-last,drop-while,empty,empty?,%
ensure,enumeration-seq,eval,even?,every?,false,false?,ffirst,file-seq,filter,finally,find,find-doc,%
find-ns,find-var,first,float,float-array,float?,floats,flush,fn,fn?,fnext,for,force,format,future,%
future-call,future-cancel,future-cancelled?,future-done?,future?,gen-class,gen-interface,gensym,%
get,get-in,get-method,get-proxy-class,get-thread-bindings,get-validator,hash,hash-map,hash-set,%
identical?,identity,if,if-let,if-not,ifn?,import,in-ns,inc,init-proxy,instance?,int,int-array,%
integer?,interleave,intern,interpose,into,into-array,ints,io!,isa?,iterate,iterator-seq,juxt,%
key,keys,keyword,keyword?,last,lazy-cat,lazy-seq,let,letfn,line-seq,list,list*,list?,load,load-file,%
load-reader,load-string,loaded-libs,locking,long,long-array,longs,loop,macroexpand,macroexpand-1,%
make-array,make-hierarchy,map,map?,mapcat,max,max-key,memfn,memoize,merge,merge-with,meta,%
method-sig,methods,min,min-key,mod,monitor-enter,monitor-exit,name,namespace,neg?,new,newline,%
next,nfirst,nil,nil?,nnext,not,not-any?,not-empty,not-every?,not=,ns,ns-aliases,ns-imports,%
ns-interns,ns-map,ns-name,ns-publics,ns-refers,ns-resolve,ns-unalias,ns-unmap,nth,nthnext,num,%
number?,odd?,or,parents,partial,partition,pcalls,peek,persistent!,pmap,pop,pop!,pop-thread-bindings,%
pos?,pr,pr-str,prefer-method,prefers,primitives-classnames,print,print-ctor,print-doc,print-dup,%
print-method,print-namespace-doc,print-simple,print-special-doc,print-str,printf,println,println-str,%
prn,prn-str,promise,proxy,proxy-call-with-super,proxy-mappings,proxy-name,proxy-super,%
push-thread-bindings,pvalues,quot,rand,rand-int,range,ratio?,rational?,rationalize,re-find,%
re-groups,re-matcher,re-matches,re-pattern,re-seq,read,read-line,read-string,recur,reduce,ref,%
ref-history-count,ref-max-history,ref-min-history,ref-set,refer,refer-clojure,reify,%
release-pending-sends,rem,remove,remove-method,remove-ns,remove-watch,repeat,repeatedly,%
replace,replicate,require,reset!,reset-meta!,resolve,rest,resultset-seq,reverse,reversible?,%
rseq,rsubseq,second,select-keys,send,send-off,seq,seq?,seque,sequence,sequential?,set,set!,%
set-validator!,set?,short,short-array,shorts,shutdown-agents,slurp,some,sort,sort-by,sorted-map,%
sorted-map-by,sorted-set,sorted-set-by,sorted?,special-form-anchor,special-symbol?,split-at,%
split-with,str,stream?,string?,struct,struct-map,subs,subseq,subvec,supers,swap!,symbol,symbol?,%
sync,syntax-symbol-anchor,take,take-last,take-nth,take-while,test,the-ns,throw,time,to-array,%
to-array-2d,trampoline,transient,tree-seq,true,true?,try,type,unchecked-add,unchecked-dec,%
unchecked-divide,unchecked-inc,unchecked-multiply,unchecked-negate,unchecked-remainder,%
unchecked-subtract,underive,unquote,unquote-splicing,update-in,update-proxy,use,val,vals,%
var,var-get,var-set,var?,vary-meta,vec,vector,vector?,when,when-first,when-let,when-not,%
while,with-bindings,with-bindings*,with-in-str,with-loading-context,with-local-vars,%
with-meta,with-open,with-out-str,with-precision,xml-seq,zero?,zipmap
},%
   sensitive,% ???
   alsodigit=-,%
   morecomment=[l];,%
   morestring=[b]"%
  }[keywords,comments,strings]%

\newcommand{\SourceCode}[4]{
  % #1 = language, usually first letter is in upper case
  % #2 = caption
  % #3 = label
  % #4 = path
  \lstset{language=#1}
  \lstset{
    commentstyle=\color{orange}\textit,
    basicstyle=\footnotesize\ttfamily, % Standardschrift
    % numbers=left,               % Ort der Zeilennummern
    numberstyle=\tiny,          % Stil der Zeilennummern
    % stepnumber=2,               % Abstand zwischen den Zeilennummern
    numbersep=5pt,              % Abstand der Nummern zum Text
    tabsize=2,                  % Groesse von Tabs
    extendedchars=true,         %
    breaklines=true,            % Zeilen werden Umgebrochen
    keywordstyle=\color{blue},
    frame=b,
    % keywordstyle=[1]\textbf,    % Stil der Keywords
    % keywordstyle=[2]\textbf,    %
    % keywordstyle=[3]\textbf,    %
    % keywordstyle=[4]\textbf,   \sqrt{\sqrt{}} %
    stringstyle=\color{blue}\ttfamily, % Farbe der String
    showspaces=false,           % Leerzeichen anzeigen ?
    showtabs=false,             % Tabs anzeigen ?
    xleftmargin=17pt,
    framexleftmargin=17pt,
    framexrightmargin=5pt,
    framexbottommargin=4pt,
    backgroundcolor=\color{listinggray},
    % backgroundcolor=\color{lightgray},
    showstringspaces=false      % Leerzeichen in Strings anzeigen ?
  }
  \lstinputlisting[caption={#2},label={#3}]{#4}
}

\newcommand{\CodeSnippet}[6]{
  % #1 = language, usually first letter is in upper case
  % #2 = caption
  % #3 = label
  % #4 = path
  % #5 = firstline
  % #6 = lastline
  \lstset{language=#1}
  \lstset{
    commentstyle=\color{orange}\textit,
    basicstyle=\footnotesize\ttfamily, % Standardschrift
    % numbers=left,               % Ort der Zeilennummern
    numberstyle=\tiny,          % Stil der Zeilennummern
    % stepnumber=2,               % Abstand zwischen den Zeilennummern
    numbersep=5pt,              % Abstand der Nummern zum Text
    tabsize=2,                  % Groesse von Tabs
    extendedchars=true,         %
    breaklines=true,            % Zeilen werden Umgebrochen
    keywordstyle=\color{blue},
    frame=tblr,
    % keywordstyle=[1]\textbf,    % Stil der Keywords
    % keywordstyle=[2]\textbf,    %
    % keywordstyle=[3]\textbf,    %
    % keywordstyle=[4]\textbf,   \sqrt{\sqrt{}} %
    stringstyle=\color{blue}\ttfamily, % Farbe der String
    showspaces=false,           % Leerzeichen anzeigen ?
    showtabs=false,             % Tabs anzeigen ?
    xleftmargin=17pt,
    framexleftmargin=17pt,
    framexrightmargin=5pt,
    framexbottommargin=4pt,
    backgroundcolor=\color{listinggray},
    % backgroundcolor=\color{lightgray},
    showstringspaces=false      % Leerzeichen in Strings anzeigen ?
  }
  \lstinputlisting[caption={#2},label={#3}, firstline={#5}, lastline={#6}]{#4}
}


% =====================
% = Section Numbering =
% =====================


%One of these two should be uncommented
% \sectionnumbering{\secnumtrue} %section numbering
\sectionnumbering{\secnumfalse} %no section numbering


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Front matter
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
