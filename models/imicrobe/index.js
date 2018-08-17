'use strict';

var fs        = require("fs");
var path      = require("path");
var sequelize = require('../../config/mysql').sequelize;


/**
 * Import database model files
 */

var models = {};

fs
    .readdirSync(__dirname)
    .filter(function(file) {
        return (file.split(".").pop() === 'js') && (file !== "index.js");
    })
    .forEach(function(file) {
        var model = sequelize.import(path.join(__dirname, file));
        models[model.name] = model;
    });

module.exports = models;


/**
 * Define table relationships (must be done manually, not auto-generated by sequelize-auto)
 */

//// project <-> investigator
//models.project.belongsToMany(models.investigator, { through: models.project_to_investigator, foreignKey: 'project_id' });
//models.investigator.belongsToMany(models.project, { through: models.project_to_investigator, foreignKey: 'investigator_id' });
//
//// project <-> project group
//models.project.belongsToMany(models.project_group, { through: models.project_to_project_group, foreignKey: 'project_id' });
//models.project_group.belongsToMany(models.project, { through: models.project_to_project_group, foreignKey: 'project_group_id' });
//
//// project <-> domain
//models.project.belongsToMany(models.domain, { through: models.project_to_domain, foreignKey: 'project_id' });
//models.domain.belongsToMany(models.project, { through: models.project_to_domain, foreignKey: 'domain_id' });
//
//// project <-> publication
//models.project.hasMany(models.publication, { foreignKey: 'project_id' });
//models.publication.belongsTo(models.project, { foreignKey: 'project_id' });
//
//// project <-> assembly
//models.project.hasMany(models.assembly, { foreignKey: 'project_id' });
//models.assembly.belongsTo(models.project, { foreignKey: 'project_id' });
//
//// project <-> combined_assembly
//models.project.hasMany(models.combined_assembly, { foreignKey: 'project_id' });
//models.combined_assembly.belongsTo(models.project, { foreignKey: 'project_id' });

// project <-> sample
models.project.hasMany(models.sample, { foreignKey: 'project_id' });
models.sample.belongsTo(models.project, { foreignKey: 'project_id' });

// project <-> user
models.project.belongsTo(models.user, { foreignKey: 'ebi_submitter_id' });

// project_file -> project_file
//models.project.hasMany(models.project_file, { foreignKey: 'project_id' });
//models.project_file.belongsTo(models.project, { foreignKey: 'project_id' });
//
//// project_file <-> project_file_type
//models.project_file_type.hasMany(models.project_file, { foreignKey: 'project_file_type_id' });
//models.project_file.belongsTo(models.project_file_type, { foreignKey: 'project_file_type_id' });
//
//// project <-> user
//models.project.belongsToMany(models.user, { through: models.project_to_user, foreignKey: 'project_id' });
//models.user.belongsToMany(models.project, { through: models.project_to_user, foreignKey: 'user_id' });
//models.project.hasMany(models.project_to_user, { foreignKey: 'project_id' });
////models.project_to_user.belongsTo(models.project, { foreignKey: 'project_id' });
//
//// project_group <-> user
//models.project_group.belongsToMany(models.user, { through: models.project_group_to_user, foreignKey: 'project_group_id' });
//models.user.belongsToMany(models.project_group, { through: models.project_group_to_user, foreignKey: 'user_id' });
//models.project_group.hasMany(models.project_group_to_user, { foreignKey: 'project_group_id' });
//
//// publication <-> project_file
//models.publication.belongsToMany(models.project_file, { through: models.publication_to_project_file, foreignKey: 'publication_id' });
//models.project_file.belongsToMany(models.publication, { through: models.publication_to_project_file, foreignKey: 'project_file_id' });

// sample <-> investigator
//models.sample.belongsToMany(models.investigator, { through: models.sample_to_investigator, foreignKey: 'sample_id' });
//models.investigator.belongsToMany(models.sample, { through: models.sample_to_investigator, foreignKey: 'investigator_id' });

//// sample <-> sample group
//models.sample.belongsToMany(models.sample_group, { through: models.sample_to_sample_group, foreignKey: 'sample_id' });
//models.sample_group.belongsToMany(models.sample, { through: models.sample_to_sample_group, foreignKey: 'sample_group_id' });
//
//// sample <-> ontology
//models.sample.belongsToMany(models.ontology, { through: models.sample_to_ontology, foreignKey: 'sample_id' });
//models.ontology.belongsToMany(models.sample, { through: models.sample_to_ontology, foreignKey: 'ontology_id' });
//
//// sample <-> assembly
//models.sample.hasMany(models.assembly, { foreignKey: 'sample_id' });
//models.assembly.belongsTo(models.sample, { foreignKey: 'sample_id' });
//
//// sample <-> combined_assembly
//models.sample.belongsToMany(models.combined_assembly, { through: models.combined_assembly_to_sample, foreignKey: 'sample_id' });
//models.combined_assembly.belongsToMany(models.sample, { through: models.combined_assembly_to_sample, foreignKey: 'combined_assembly_id' });

// sample <- sample_file
models.sample.hasMany(models.sample_file, { foreignKey: 'sample_id' });
models.sample_file.belongsTo(models.sample, { foreignKey: 'sample_id' });

// sample_file <- sample_file_type
models.sample_file.belongsTo(models.sample_file_type, { foreignKey: 'sample_file_type_id' });

// sample <- sample_attr
models.sample.hasMany(models.sample_attr, { foreignKey: 'sample_id' });
models.sample_attr.belongsTo(models.sample, { foreignKey: 'sample_id' });

// sample_attr <-> sample_attr_type
models.sample_attr_type.hasMany(models.sample_attr, { foreignKey: 'sample_attr_type_id' });
models.sample_attr.belongsTo(models.sample_attr_type, { foreignKey: 'sample_attr_type_id' });

// sample_attr_type -> sample_attr_type_alias
models.sample_attr_type.hasMany(models.sample_attr_type_alias, { foreignKey: 'sample_attr_type_id' });

// sample_attr_type -> sample_attr_type_category
models.sample_attr_type.belongsTo(models.sample_attr_type_category, { foreignKey: 'sample_attr_type_category_id' });

//// sample <-> uproc pfam
//models.sample.belongsToMany(models.pfam_annotation, { through: models.uproc_pfam_result, foreignKey: 'sample_id' });
//models.uproc_pfam_result.belongsTo(models.sample, { foreignKey: 'sample_id' });
//models.pfam_annotation.belongsToMany(models.sample, { through: models.uproc_pfam_result, foreignKey: 'uproc_id' });
//models.uproc_pfam_result.belongsTo(models.pfam_annotation, { foreignKey: 'uproc_id' });
//models.pfam_annotation.hasMany(models.uproc_pfam_result, { foreignKey: 'uproc_id' });
//
//// sample <-> uproc kegg
//models.sample.belongsToMany(models.kegg_annotation, { through: models.uproc_kegg_result, foreignKey: 'sample_id' });
//models.uproc_kegg_result.belongsTo(models.sample, { foreignKey: 'sample_id' });
//models.kegg_annotation.belongsToMany(models.sample, { through: models.uproc_kegg_result, foreignKey: 'kegg_annotation_id' });
//models.uproc_kegg_result.belongsTo(models.kegg_annotation, { foreignKey: 'kegg_annotation_id' });
//models.kegg_annotation.hasMany(models.uproc_kegg_result, { foreignKey: 'kegg_annotation_id' });
//
//// sample <-> centrifuge
//models.sample.belongsToMany(models.centrifuge, { through: models.sample_to_centrifuge, foreignKey: 'sample_id' });
//models.centrifuge.belongsToMany(models.sample, { through: models.sample_to_centrifuge, foreignKey: 'centrifuge_id' });
//models.sample_to_centrifuge.belongsTo(models.centrifuge, { foreignKey: 'centrifuge_id' });
//
//// app <-> app_run
//models.app.hasMany(models.app_run, { foreignKey: 'app_id' });
//models.app_run.belongsTo(models.app, { foreignKey: 'app_id' });
//
//// app <-> app_tag
//models.app.belongsToMany(models.app_tag, { through: models.app_to_app_tag, foreignKey: 'app_id' });
//models.app_tag.belongsToMany(models.app, { through: models.app_to_app_tag, foreignKey: 'app_tag_id' });
//
//// app <-> app_data_type
//models.app.belongsToMany(models.app_data_type, { through: models.app_to_app_data_type, foreignKey: 'app_id' });
//models.app_data_type.belongsToMany(models.app, { through: models.app_to_app_data_type, foreignKey: 'app_data_type_id' });
//
//// app <-> app_result
//models.app.hasMany(models.app_result, { foreignKey: 'app_id' });
//models.app_result.belongsTo(models.app, { foreignKey: 'app_id' });
//
//// app_result <-> app_data_type
//models.app_data_type.hasMany(models.app_result, { foreignKey: 'app_data_type_id'} );
//models.app_result.belongsTo(models.app_data_type, { foreignKey: 'app_data_type_id'} );


models.getProject = function(projectId) {
    return models.project.findOne({
        where: { project_id: projectId },
        include: [
            { model: models.sample,
              include: [
                { model: models.sample_file,
                  include: [
                    { model: models.sample_file_type
                    }
                  ]
                },
                { model: models.sample_attr,
                  include: [
                    { model: models.sample_attr_type
                    }
                  ]
                }
              ]
            }
        ]
    });
}